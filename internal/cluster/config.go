package cluster

import (
	"encoding/json"
	"fmt"
	"os"
)

type ClusterConfig struct {
	Nodes []NodeConfig `json:"nodes"`
}

type NodeConfig struct {
	NodeId   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr"`
	DataDir  string `json:"data_dir"`
	JoinAddr string `json:"join_addr"`
}

func LoadClusterConfig(configPath string) (*ClusterConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

func (c *ClusterConfig) GetNodeConfig(nodeId string) (*NodeConfig, error) {
	for _, node := range c.Nodes {
		if node.NodeId == nodeId {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("node %s not found in config", nodeId)
}

func (c *ClusterConfig) GetBootstrapNode() *NodeConfig {
	if len(c.Nodes) > 0 {
		return &c.Nodes[0]
	}
	return nil
}

func (c *ClusterConfig) IsBootstrapNode(nodeId string) bool {
	bootstrap := c.GetBootstrapNode()
	return bootstrap != nil && bootstrap.NodeId == nodeId
}
