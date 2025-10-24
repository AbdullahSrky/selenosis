package selenosis

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"regexp"
	"strings"
	"time"

	"github.com/alcounit/selenosis/platform"
	"github.com/alcounit/selenosis/selenium"
	"github.com/alcounit/selenosis/tools"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/imdario/mergo"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
)

var (
	httpClient = &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			DisableKeepAlives:     false,
			ResponseHeaderTimeout: 10 * time.Minute,
		},
		Timeout: 0, // No timeout - let browser's pageLoad timeout control this
	}
	dialer net.Dialer
)

const browser = "browser"

type capabilities struct {
	DesiredCapabilities selenium.Capabilities `json:"desiredCapabilities"`
	Capabilities        struct {
		AlwaysMatch selenium.Capabilities    `json:"alwaysMatch"`
		FirstMatch  []*selenium.Capabilities `json:"firstMatch"`
	} `json:"capabilities"`
}

type Status struct {
	Total    int                 `json:"total"`
	Active   int                 `json:"active"`
	Pending  int                 `json:"pending"`
	Browsers map[string][]string `json:"config,omitempty"`
	Sessions []platform.Service  `json:"sessions,omitempty"`
}

type response struct {
	Status    int    `json:"status"`
	Version   string `json:"version"`
	Error     string `json:"err,omitempty"`
	Selenosis Status `json:"selenosis,omitempty"`
}

// HandleSession ...
func (app *App) HandleSession(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	logger := app.logger.WithFields(logrus.Fields{
		"request_id": uuid.New(),
		"request":    fmt.Sprintf("%s %s", r.Method, r.URL.Path),
	})
	logger.WithField("time_elapsed", tools.TimeElapsed(start)).Info("session")

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("failed to read request body: %v", err)
		tools.JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	request := capabilities{}

	err = json.Unmarshal(body, &request)
	if err != nil {
		logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("failed to parse request: %v", err)
		tools.JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.Capabilities.AlwaysMatch.GetBrowserName() != "" && request.DesiredCapabilities.GetBrowserName() == "" {
		request.DesiredCapabilities = request.Capabilities.AlwaysMatch
	}

	firstMatchCaps := request.Capabilities.FirstMatch
	if len(firstMatchCaps) == 0 {
		firstMatchCaps = append(firstMatchCaps, &selenium.Capabilities{})
	}

	var browser platform.BrowserSpec
	var caps selenium.Capabilities

	for _, fmc := range firstMatchCaps {
		caps = request.DesiredCapabilities
		mergo.Merge(&caps, *fmc)
		caps.ValidateCapabilities()

		browser, err = app.browsers.Find(caps.GetBrowserName(), caps.BrowserVersion)
		if err == nil {
			break
		}
	}

	if err != nil {
		logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("requested browser not found: %v", err)
		tools.JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	logger.WithField("time_elapsed", tools.TimeElapsed(start)).Infof("starting browser from image: %s", browser.Image)

	image := parseImage(browser.Image)

	var service platform.Service
	j := 1
	for ; ; j++ {
		service, err = app.client.Service().Create(platform.ServiceSpec{
			SessionID:             fmt.Sprintf("%s-%s", image, uuid.New()),
			RequestedCapabilities: caps,
			Template:              browser,
		})
		if err != nil {
			logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("failed to start browser: %v", err)
			if j < app.sessionRetryCount {
				continue
			}
			tools.JSONError(w, "failed to start browser: "+err.Error(), http.StatusBadRequest)
			return
		}
		break
	}

	cancel := func() {
		service.CancelFunc()
	}

	var resp *http.Response

	service.URL.Path = r.URL.Path

	i := 1
	for ; ; i++ {
		req, _ := http.NewRequest(http.MethodPost, service.URL.String(), bytes.NewReader(body))
		req.Close = true
		req.Header.Set("X-Forwarded-Selenosis", app.selenosisHost)
		ctx, done := context.WithTimeout(r.Context(), app.browserWaitTimeout)
		rsp, err := httpClient.Do(req.WithContext(ctx))
		defer done()
		select {
		case <-ctx.Done():
			if rsp != nil {
				rsp.Body.Close()
			}
			switch ctx.Err() {
			case context.DeadlineExceeded:
				logger.WithField("time_elapsed", tools.TimeElapsed(start)).Warn("session attempt timeout")
				if i < app.sessionRetryCount {
					logger.WithField("time_elapsed", tools.TimeElapsed(start)).Warnf("session retrying: %d/%d", i, app.sessionRetryCount)
					continue
				}
				logger.WithField("time_elapsed", tools.TimeElapsed(start)).Warn("service is not ready")
				tools.JSONError(w, "New session attempts retry count exceeded", http.StatusInternalServerError)
			case context.Canceled:
				logger.WithField("time_elapsed", tools.TimeElapsed(start)).Warn("Client disconnected")
			}
			cancel()
			return
		default:
		}
		if err != nil {
			logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("session failed: %v", err)
			if i < app.sessionRetryCount {
				logger.WithField("time_elapsed", tools.TimeElapsed(start)).Warnf("session retrying for session failed: %d/%d", i, app.sessionRetryCount)
				continue
			}
			tools.JSONError(w, "New session attempts retry count exceeded", http.StatusInternalServerError)
			cancel()
			return
		}
		if rsp.StatusCode == http.StatusNotFound {
			continue
		}
		resp = rsp
		break
	}
	defer resp.Body.Close()

	var msg map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&msg)
	if err != nil {
		cancel()
		logger.WithField("time_elapsed", tools.TimeElapsed(start)).Errorf("unable to read service response: %v", err)
		tools.JSONError(w, "Failed to read service response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(msg)

	logger.WithField("time_elapsed", tools.TimeElapsed(start)).Infof("browser sessionId: %s", service.SessionID)

}

// HandleProxy ...
func (app *App) HandleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	sessionID, ok := mux.Vars(r)["sessionId"]
	if !ok {
		app.logger.WithField("request", fmt.Sprintf("%s %s", r.Method, r.URL.Path)).Error("session id not found")
		tools.JSONError(w, "session id not found", http.StatusBadRequest)
		return
	}

	if !isValidSession(sessionID) {
		app.logger.WithField("request", fmt.Sprintf("%s %s", r.Method, r.URL.Path)).Errorf("%s is not valid session id", sessionID)
		tools.JSONError(w, "session id not found", http.StatusBadRequest)
		return
	}

	logger := app.logger.WithFields(logrus.Fields{
		"request_id": uuid.New(),
		"session_id": sessionID,
		"request":    fmt.Sprintf("%s %s", r.Method, r.URL.Path),
	})

	// Read request body once
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.WithError(err).Error("Failed to read request body")
		tools.JSONError(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// Prepare target URL
	r.URL.Scheme = "http"
	r.Host = sessionID + "." + app.serviceName + ":" + app.sidecarPort
	r.URL.Host = r.Host
	r.Header.Set("X-Forwarded-Selenosis", app.selenosisHost)

	// Retry logic with exponential backoff
	maxRetries := app.requestRetryCount
	if maxRetries <= 0 {
		maxRetries = 1 // At least one attempt
	}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Clone request for each attempt
		rCopy := r.Clone(r.Context())
		rCopy.Body = ioutil.NopCloser(bytes.NewReader(body))

		// Track if proxy succeeded
		proxySucceeded := true

		proxy := &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// Log only on first attempt or errors
				if attempt == 1 || lastErr != nil {
					logger.WithFields(logrus.Fields{
						"attempt": attempt,
						"elapsed": time.Since(start),
					}).Debug("Proxying request to browser")
				}
			},
			ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
				proxySucceeded = false
				lastErr = err

				logger.WithFields(logrus.Fields{
					"attempt":     attempt,
					"max_retries": maxRetries,
					"elapsed":     time.Since(start),
					"error":       err.Error(),
				}).Warn("Proxy request failed")

				// Only send error response on final attempt
				if attempt >= maxRetries {
					if strings.Contains(err.Error(), "no such host") {
						tools.JSONError(w, "Browser session not found: "+err.Error(), http.StatusNotFound)
					} else if strings.Contains(err.Error(), "connection refused") {
						tools.JSONError(w, "Browser not responding: "+err.Error(), http.StatusServiceUnavailable)
					} else if strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "timeout") {
						tools.JSONError(w, "Browser request timeout: "+err.Error(), http.StatusGatewayTimeout)
					} else {
						tools.JSONError(w, "Proxy error: "+err.Error(), http.StatusBadGateway)
					}
				}
			},
		}

		proxy.ServeHTTP(w, rCopy)

		// If proxy succeeded, we're done
		if proxySucceeded {
			if attempt > 1 {
				logger.WithFields(logrus.Fields{
					"attempt": attempt,
					"elapsed": time.Since(start),
				}).Info("Proxy request succeeded after retry")
			}
			return
		}

		// If not final attempt, wait before retry
		if attempt < maxRetries {
			// Exponential backoff: 100ms, 200ms, 400ms, 800ms, ...
			backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond
			if backoff > 2*time.Second {
				backoff = 2 * time.Second // Cap at 2 seconds
			}

			logger.WithFields(logrus.Fields{
				"attempt":      attempt,
				"next_attempt": attempt + 1,
				"backoff":      backoff,
			}).Debug("Retrying proxy request after backoff")

			time.Sleep(backoff)
		}
	}

	// All retries exhausted
	logger.WithFields(logrus.Fields{
		"attempts":   maxRetries,
		"elapsed":    time.Since(start),
		"last_error": lastErr,
	}).Error("Proxy request failed after all retries")
}

// HandleHubStatus ...
func (app *App) HandleHubStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(
		map[string]interface{}{
			"value": map[string]interface{}{
				"message": "selenosis up and running",
				"ready":   len(app.stats.Sessions().List()),
			},
		})
}

// HandleReverseProxy ...
func (app *App) HandleReverseProxy(w http.ResponseWriter, r *http.Request) {
	sessionID, ok := mux.Vars(r)["sessionId"]
	if !ok {
		app.logger.WithField("request", fmt.Sprintf("%s %s", r.Method, r.URL.Path)).Error("session id not found")
		tools.JSONError(w, "session id not found", http.StatusBadRequest)
		return
	}

	if !isValidSession(sessionID) {
		app.logger.WithField("request", fmt.Sprintf("%s %s", r.Method, r.URL.Path)).Errorf("%s is not valid session id", sessionID)
		tools.JSONError(w, "session id not found", http.StatusBadRequest)
		return
	}

	logger := app.logger.WithFields(logrus.Fields{
		"request_id": uuid.New(),
		"session_id": sessionID,
		"request":    fmt.Sprintf("%s %s", r.Method, r.URL.Path),
	})

	fragments := strings.Split(r.URL.Path, "/")
	(&httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = "http"
			r.Host = sessionID + "." + app.serviceName + ":" + app.sidecarPort
			r.URL.Host = r.Host
			r.Header.Set("X-Forwarded-Selenosis", app.selenosisHost)
			logger.Infof("proxying %s", fragments[1])
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Errorf("%s proxying error: %v", fragments[1], err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}).ServeHTTP(w, r)
}

// HandleVNC ...
func (app *App) HandleVNC() websocket.Handler {
	return func(wsconn *websocket.Conn) {
		defer wsconn.Close()

		sessionID, ok := mux.Vars(wsconn.Request())["sessionId"]
		if !ok {
			app.logger.WithField("request", fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path)).Error("session id not found")
			return
		}

		if !isValidSession(sessionID) {
			app.logger.WithField("request", fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path)).Errorf("%s is not valid session id", sessionID)
			return
		}

		host := tools.BuildHostPort(sessionID, app.serviceName, "5900")
		logger := app.logger.WithFields(logrus.Fields{
			"request_id": uuid.New(),
			"session_id": sessionID,
			"request":    fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path),
		})
		logger.Infof("vnc request: %s", host)

		conn, err := dialer.DialContext(wsconn.Request().Context(), "tcp", host)
		if err != nil {
			logger.Errorf("vnc connection error: %v", err)
			return
		}
		defer conn.Close()

		wsconn.PayloadType = websocket.BinaryFrame
		go func() {
			io.Copy(wsconn, conn)
			logger.Warnf("vnc connection closed")
		}()
		io.Copy(conn, wsconn)
		logger.Infof("vnc client disconnected")
	}
}

// HandleLogs ...
func (app *App) HandleLogs() websocket.Handler {
	return func(wsconn *websocket.Conn) {
		defer wsconn.Close()

		sessionID, ok := mux.Vars(wsconn.Request())["sessionId"]
		if !ok {
			app.logger.WithField("request", fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path)).Error("session id not found")
			return
		}

		if !isValidSession(sessionID) {
			app.logger.WithField("request", fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path)).Errorf("%s is not valid session id", sessionID)
			return
		}

		logger := app.logger.WithFields(logrus.Fields{
			"request_id": uuid.New(),
			"session_id": sessionID,
			"request":    fmt.Sprintf("%s %s", wsconn.Request().Method, wsconn.Request().URL.Path),
		})
		logger.Infof("stream logs request: %s", fmt.Sprintf("%s.%s", sessionID, app.serviceName))

		conn, err := app.client.Service().Logs(wsconn.Request().Context(), sessionID)
		if err != nil {
			logger.Errorf("stream logs error: %v", err)
			return
		}
		defer conn.Close()

		wsconn.PayloadType = websocket.BinaryFrame
		go func() {
			io.Copy(wsconn, conn)
			wsconn.Close()
			logger.Warnf("stream logs connection closed")
		}()
		io.Copy(wsconn, conn)
		logger.Infof("stream logs disconnected")
	}
}

// HandleStatus ...
func (app *App) HandleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var active []platform.Service
	var pending int
	for _, s := range app.stats.Sessions().List() {
		switch s.Status {
		case platform.Running:
			s.Uptime = tools.TimeElapsed(s.Started)
			active = append(active, s)
		case platform.Pending:
			pending++
		}
	}

	json.NewEncoder(w).Encode(
		response{
			Status:  http.StatusOK,
			Version: app.buildVersion,
			Selenosis: Status{
				Total:    app.sessionLimit,
				Active:   len(active),
				Pending:  pending,
				Browsers: app.browsers.GetBrowserVersions(),
				Sessions: active,
			},
		},
	)
}

func parseImage(image string) (container string) {
	if len(image) > 0 {
		pref, err := regexp.Compile("[^a-zA-Z0-9]+")
		if err != nil {
			return browser
		}
		fragments := strings.Split(image, "/")
		image = fragments[len(fragments)-1]
		return pref.ReplaceAllString(image, "-")
	}
	return browser
}

func isValidSession(session string) bool {
	/*
		A UUID is made up of hex digits (4 chars each) along with 4 "- symbols,
		which make its length equal to 36 characters.
	*/

	sLen := len(session)

	if sLen >= 36 {
		switch sLen {
		case 36:
			_, err := uuid.Parse(session)
			return err == nil
		default:
			sess := session[len(session)-36:]
			_, err := uuid.Parse(sess)
			return err == nil
		}
	}
	return false
}
