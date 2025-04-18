// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package elasticsearch

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/v8/channels/audit"
	"github.com/mattermost/mattermost/server/v8/channels/web"
)

// API defines handlers for Elasticsearch API endpoints
type API struct {
	service *Service
	root    *mux.Router
}

// Init configures the API routes
func (api *API) Init(root *mux.Router, service *Service) {
	api.service = service
	api.root = root

	// Configure endpoints
	searchRouter := root.PathPrefix("/api/v4/search/es").Subrouter()
	searchRouter.Handle("", api.ApiSessionRequired(searchPostsES)).Methods("GET")
	
	// Management API endpoints
	adminRouter := root.PathPrefix("/api/v4/elasticsearch").Subrouter()
	adminRouter.Handle("/index_batch", api.ApiSessionRequired(api.ApiRequireSystemAdmin(indexPostBatch))).Methods("POST")
	adminRouter.Handle("/test_config", api.ApiSessionRequired(api.ApiRequireSystemAdmin(testElasticsearchConfig))).Methods("POST")
}

// ApiHandler provides a wrapper for http.Handler with authentication for APIs
func (api *API) ApiHandler(h func(*web.Context, http.ResponseWriter, *http.Request)) http.Handler {
	return &web.Handler{
		HandleFunc:     h,
		RequireSession: false,
		TrustRequester: false,
		RequireMfa:     false,
		IsStatic:       false,
	}
}

// ApiSessionRequired provides a wrapper for http.Handler requiring authenticated sessions
func (api *API) ApiSessionRequired(h func(*web.Context, http.ResponseWriter, *http.Request)) http.Handler {
	return &web.Handler{
		HandleFunc:     h,
		RequireSession: true,
		TrustRequester: false,
		RequireMfa:     true,
		IsStatic:       false,
	}
}

// ApiRequireSystemAdmin enforces system admin permissions for API endpoints
func (api *API) ApiRequireSystemAdmin(h func(*web.Context, http.ResponseWriter, *http.Request)) func(*web.Context, http.ResponseWriter, *http.Request) {
	return func(c *web.Context, w http.ResponseWriter, r *http.Request) {
		if !c.App.SessionHasPermissionTo(*c.AppContext.Session(), model.PermissionManageSystem) {
			c.Err = model.NewAppError("ApiRequireSystemAdmin", "api.context.system_permissions_required.app_error", nil, "", http.StatusForbidden)
			return
		}
		h(c, w, r)
	}
}

// searchPostsES handles post search requests using Elasticsearch
func searchPostsES(c *web.Context, w http.ResponseWriter, r *http.Request) {
	// Verify service availability
	esService := c.App.Srv().ElasticsearchService()
	if esService == nil || !esService.IsActive() {
		c.Err = model.NewAppError("searchPostsES", "api.elasticsearch.search_posts.service_disabled", nil, "", http.StatusNotImplemented)
		return
	}
	
	// Extract search query
	query := r.URL.Query().Get("q")
	if query == "" {
		c.SetInvalidParam("q")
		return
	}
	
	// Extract pagination parameters
	page := 0
	if r.URL.Query().Get("page") != "" {
		var err error
		page, err = model.ParseInt(r.URL.Query().Get("page"), 10)
		if err != nil {
			c.SetInvalidParam("page")
			return
		}
	}
	
	perPage := 20
	if r.URL.Query().Get("per_page") != "" {
		var err error
		perPage, err = model.ParseInt(r.URL.Query().Get("per_page"), 10)
		if err != nil {
			c.SetInvalidParam("per_page")
			return
		}
	}
	
	// Check search type
	isOrSearch := false
	if r.URL.Query().Get("is_or_search") == "true" {
		isOrSearch = true
	}
	
	// Setup audit logging
	auditRec := c.App.MakeAuditRecord("searchPostsES", audit.Fail)
	defer c.App.LogAuditRec(auditRec, c.AppContext.Session().UserId)
	audit.AddEventParameter(auditRec, "query", query)
	
	// Track operation timing
	startTime := time.Now()
	
	// Prepare search parameters
	params := model.ParseSearchParams(query, 0)
	for _, param := range params {
		param.OrTerms = isOrSearch
	}
	
	teamId := ""
	if len(c.AppContext.Session().TeamIds) > 0 {
		teamId = c.AppContext.Session().TeamIds[0]
	}
	
	// Execute search operation
	results, err := c.App.SearchPostsForUser(c.AppContext, query, c.AppContext.Session().UserId, teamId, isOrSearch, false, 0, page, perPage)
	
	elapsedTime := float64(time.Since(startTime)) / float64(time.Second)
	metrics := c.App.Metrics()
	if metrics != nil {
		metrics.IncrementPostsSearchCounter()
		metrics.ObservePostsSearchDuration(elapsedTime)
	}
	
	if err != nil {
		c.Err = err
		return
	}
	
	clientPostList := c.App.PreparePostListForClient(c.AppContext, results.PostList)
	clientPostList, err = c.App.SanitizePostListMetadataForUser(c.AppContext, clientPostList, c.AppContext.Session().UserId)
	if err != nil {
		c.Err = err
		return
	}
	
	auditRec.Success()
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Write([]byte(clientPostList.ToJson()))
}

// testElasticsearchConfig handles configuration test requests
func testElasticsearchConfig(c *web.Context, w http.ResponseWriter, r *http.Request) {
	var config model.Config
	if jsonErr := json.NewDecoder(r.Body).Decode(&config); jsonErr != nil {
		c.Err = model.NewAppError("testElasticsearchConfig", "api.elasticsearch.test_config.parse_config.app_error", nil, jsonErr.Error(), http.StatusBadRequest)
		return
	}
	
	// Attempt connection with configuration
	esService := c.App.Srv().ElasticsearchService()
	if esService == nil {
		c.Err = model.NewAppError("testElasticsearchConfig", "api.elasticsearch.test_config.service_not_initialized", nil, "", http.StatusInternalServerError)
		return
	}
	
	err := esService.TestConnection(&config)
	if err != nil {
		c.Err = model.NewAppError("testElasticsearchConfig", "api.elasticsearch.test_config.connection_error", nil, err.Error(), http.StatusBadRequest)
		return
	}
	
	w.Write([]byte(model.MapToJson(map[string]string{"status": "OK"})))
}

// indexPostBatch processes batch indexing requests
func indexPostBatch(c *web.Context, w http.ResponseWriter, r *http.Request) {
	c.RequireTeamId()
	if c.Err != nil {
		return
	}
	
	var options struct {
		StartTime int64 `json:"start_time"`
		EndTime   int64 `json:"end_time"`
		Limit     int   `json:"limit"`
	}
	
	if jsonErr := json.NewDecoder(r.Body).Decode(&options); jsonErr != nil {
		c.Err = model.NewAppError("indexPostBatch", "api.elasticsearch.index_post_batch.parse_options.app_error", nil, jsonErr.Error(), http.StatusBadRequest)
		return
	}
	
	// Access search service
	esService := c.App.Srv().ElasticsearchService()
	if esService == nil || !esService.IsActive() {
		c.Err = model.NewAppError("indexPostBatch", "api.elasticsearch.index_post_batch.service_disabled", nil, "", http.StatusInternalServerError)
		return
	}
	
	// Apply default values where needed
	if options.Limit <= 0 {
		options.Limit = 1000
	}
	
	if options.StartTime <= 0 {
		// Default to historical start point
		options.StartTime = model.GetMillisForTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	}
	
	if options.EndTime <= 0 {
		// Default to current time
		options.EndTime = model.GetMillis()
	}
	
	// Retrieve posts to index
	posts, err := c.App.GetPostsForIndexing(c.AppContext, options.StartTime, options.EndTime, options.Limit)
	if err != nil {
		c.Err = model.NewAppError("indexPostBatch", "api.elasticsearch.index_post_batch.get_posts.app_error", nil, err.Error(), http.StatusInternalServerError)
		return
	}
	
	if len(posts) == 0 {
		w.Write([]byte(model.MapToJson(map[string]interface{}{
			"status": "OK",
			"count":  0,
		})))
		return
	}
	
	// Submit posts for indexing
	if err := esService.BatchIndexPosts(posts, c.Params.TeamId); err != nil {
		c.Err = model.NewAppError("indexPostBatch", "api.elasticsearch.index_post_batch.index_error", nil, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Ensure index is up-to-date
	if err := esService.RefreshIndex(); err != nil {
		c.App.GetLogger().Error("Error refreshing Elasticsearch index", mlog.Err(err))
	}
	
	w.Write([]byte(model.MapToJson(map[string]interface{}{
		"status": "OK",
		"count":  len(posts),
	})))
}