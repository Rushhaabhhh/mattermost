// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.enterprise for license information.

package elasticsearch

import (
	"fmt"
	"sync"
	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
)

// Service represents the Elasticsearch service implementation
type Service struct {
	client      *Client
	logger      mlog.LoggerIFace
	operational bool
	lock        sync.RWMutex
	configStore *model.Config
}

// New initializes a fresh Elasticsearch service instance
func New(configStore *model.Config, logger mlog.LoggerIFace) (*Service, error) {
	logger = logger.With(mlog.String("service", "elasticsearch"))

	svc := &Service{
		logger:      logger,
		configStore: configStore,
	}

	// Skip connection attempt if Elasticsearch indexing is disabled
	if !*configStore.ElasticsearchSettings.EnableIndexing {
		return svc, nil
	}

	client, err := NewClient(configStore, logger)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize elasticsearch client: %w", err)
	}

	svc.client = client
	svc.operational = client.IsReady()

	// Initialize message index if not present
	if svc.operational {
		if err := client.CreateMessageIndex(); err != nil {
			return nil, fmt.Errorf("unable to initialize elasticsearch index: %w", err)
		}
	}

	return svc, nil
}

// IsActive determines whether the service is currently operational
func (s *Service) IsActive() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.operational && s.client != nil && s.client.IsReady()
}

// UpdateConfig refreshes the service configuration
func (s *Service) UpdateConfig(config *model.Config) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if !*config.ElasticsearchSettings.EnableIndexing {
		s.operational = false
		return nil
	}

	// Initialize client if none exists
	if s.client == nil {
		client, err := NewClient(config, s.logger)
		if err != nil {
			return fmt.Errorf("unable to initialize elasticsearch client: %w", err)
		}
		s.client = client
		s.operational = client.IsReady()
	} else {
		// Refresh existing client configuration
		if err := s.client.UpdateConfig(config); err != nil {
			return fmt.Errorf("unable to refresh elasticsearch client: %w", err)
		}
		s.operational = s.client.IsReady()
	}

	// Initialize message index if not present
	if s.operational {
		if err := s.client.CreateMessageIndex(); err != nil {
			return fmt.Errorf("unable to initialize elasticsearch index: %w", err)
		}
	}

	s.configStore = config
	return nil
}

// IndexPost adds a post to the search index
func (s *Service) IndexPost(post *model.Post, teamId string) error {
	if !s.IsActive() {
		return fmt.Errorf("elasticsearch service not operational")
	}

	return s.client.IndexPost(post, teamId)
}

// DeletePost removes a post from the search index
func (s *Service) DeletePost(postId string) error {
	if !s.IsActive() {
		return fmt.Errorf("elasticsearch service not operational")
	}

	return s.client.DeletePost(postId)
}

// BatchIndexPosts adds multiple posts to the search index at once
func (s *Service) BatchIndexPosts(posts []*model.Post, teamId string) error {
	if !s.IsActive() {
		return fmt.Errorf("elasticsearch service not operational")
	}

	return s.client.BatchIndexPosts(posts, teamId)
}

// SearchPosts queries for posts matching specified criteria
func (s *Service) SearchPosts(channels model.ChannelList, searchParams []*model.SearchParams, page, perPage int) ([]string, model.PostSearchMatches, error) {
	if !s.IsActive() {
		return nil, nil, fmt.Errorf("elasticsearch service not operational")
	}

	return s.client.SearchPosts(channels, searchParams, page, perPage)
}

// TestConnection verifies connectivity to Elasticsearch
func (s *Service) TestConnection(config *model.Config) error {
	// Create temporary client for connection verification
	client := &Client{
		config: config,
		logger: s.logger,
	}

	return client.Connect()
}

// RefreshIndex forces an immediate update of the Elasticsearch index
func (s *Service) RefreshIndex() error {
	if !s.IsActive() {
		return fmt.Errorf("elasticsearch service not operational")
	}

	return s.client.RefreshIndex()
}