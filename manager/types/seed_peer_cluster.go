/*
 *     Copyright 2022 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

type SeedPeerClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSeedPeerToSeedPeerClusterParams struct {
	ID         uint `uri:"id" binding:"required"`
	SeedPeerID uint `uri:"seed_peer_id" binding:"required"`
}

type AddSchedulerClusterToSeedPeerClusterParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type CreateSeedPeerClusterRequest struct {
	Name      string                 `json:"name" binding:"required"`
	BIO       string                 `json:"bio" binding:"omitempty"`
	Config    *SeedPeerClusterConfig `json:"config" binding:"required"`
	Scopes    *SeedPeerClusterScopes `json:"scopes" binding:"omitempty"`
	IsDefault bool                   `json:"is_default" binding:"omitempty"`
}

type UpdateSeedPeerClusterRequest struct {
	Name      string                 `json:"name" binding:"omitempty"`
	BIO       string                 `json:"bio" binding:"omitempty"`
	Config    *SeedPeerClusterConfig `json:"config" binding:"omitempty"`
	Scopes    *SeedPeerClusterScopes `json:"scopes" binding:"omitempty"`
	IsDefault bool                   `json:"is_default" binding:"omitempty"`
}

type GetSeedPeerClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}

type SeedPeerClusterConfig struct {
	LoadLimit uint32 `yaml:"loadLimit" mapstructure:"loadLimit" json:"load_limit" binding:"omitempty,gte=1,lte=5000"`
}

type SeedPeerClusterScopes struct {
	IDC         string `yaml:"idc" mapstructure:"idc" json:"idc" binding:"omitempty"`
	NetTopology string `yaml:"net_topology" mapstructure:"net_topology" json:"net_topology" binding:"omitempty"`
	Location    string `yaml:"location" mapstructure:"location" json:"location" binding:"omitempty"`
}
