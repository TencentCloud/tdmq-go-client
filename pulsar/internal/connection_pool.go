// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TencentCloud/tdmq-go-client/pulsar/internal/authcloud"

	"github.com/TencentCloud/tdmq-go-client/pulsar/internal/auth"

	log "github.com/sirupsen/logrus"
)

// ConnectionPool is a interface of connection pool.
type ConnectionPool interface {
	// GetConnection get a connection from ConnectionPool.
	GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error)

	// Close all the connections in the pool
	Close()
}

type connectionPool struct {
	//for Tencent tdmq
	authCloud authcloud.AuthenticationCloud
	//original
	pool              sync.Map
	connectionTimeout time.Duration
	tlsOptions        *TLSOptions
	auth              auth.Provider
	patchConnURL      func(*url.URL)

	maxConnectionsPerHost int32
	roundRobinCnt         int32
}

// NewConnectionPool init connection pool.
func NewConnectionPool(
	tlsOptions *TLSOptions,
	auth auth.Provider,
	connectionTimeout time.Duration,
	maxConnectionsPerHost int,
	patchConnURL func(*url.URL)) ConnectionPool {
	return &connectionPool{
		tlsOptions:            tlsOptions,
		auth:                  auth,
		connectionTimeout:     connectionTimeout,
		maxConnectionsPerHost: int32(maxConnectionsPerHost),
		patchConnURL:          patchConnURL,
	}
}

func NewConnectionPoolWithAuthCloud(
	authCloud authcloud.AuthenticationCloud,
	tlsOptions *TLSOptions, auth auth.Provider,
	connectionTimeout time.Duration,
	maxConnectionsPerHost int,
	patchConnURL func(*url.URL)) ConnectionPool {
	return &connectionPool{
		authCloud:             authCloud,
		connectionTimeout:     connectionTimeout,
		tlsOptions:            tlsOptions,
		auth:                  auth,
		maxConnectionsPerHost: int32(maxConnectionsPerHost),
		patchConnURL:          patchConnURL,
	}
}

func (p *connectionPool) GetConnection(logicalAddr *url.URL, physicalAddr *url.URL) (Connection, error) {
	if p.patchConnURL != nil {
		p.patchConnURL(physicalAddr)
	}

	key := p.getMapKey(logicalAddr)
	cachedCnx, found := p.pool.Load(key)
	if found {
		cnx := cachedCnx.(*connection)
		log.Debug("Found connection in cache:", cnx.logicalAddr, cnx.physicalAddr)

		if err := cnx.waitUntilReady(); err == nil {
			// Connection is ready to be used
			return cnx, nil
		}
		// The cached connection is failed
		p.pool.Delete(key)
		log.Debug("Removed failed connection from pool:", cnx.logicalAddr, cnx.physicalAddr)
	}

	// Try to create a new connection
	newConn := newConnectionAuthCloud(logicalAddr, physicalAddr, p.tlsOptions, p.connectionTimeout, p.auth, p.authCloud)
	newCnx, wasCached := p.pool.LoadOrStore(key, newConn)
	cnx := newCnx.(*connection)

	if !wasCached {
		cnx.start()
	} else {
		newConn.Close()
	}

	if err := cnx.waitUntilReady(); err != nil {
		return nil, err
	}
	return cnx, nil
}

func (p *connectionPool) Close() {
	p.pool.Range(func(key, value interface{}) bool {
		value.(Connection).Close()
		return true
	})
}

func (p *connectionPool) getMapKey(addr *url.URL) string {
	cnt := atomic.AddInt32(&p.roundRobinCnt, 1)
	if cnt < 0 {
		cnt = -cnt
	}
	idx := cnt % p.maxConnectionsPerHost
	return fmt.Sprint(addr.Host, '-', idx)
}
