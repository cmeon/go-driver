//
// DISCLAIMER
//
// Copyright 2017 ArangoDB GmbH, Cologne, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright holder is ArangoDB GmbH, Cologne, Germany
//
// Author Ewout Prangsma
//

package driver

import (
	"context"
	"fmt"
	"path"
	"strings"
)

const (
	keyTransactionWaitForSync   = "arangodb-transaction-waitForSync"
	keyTransactionLockTimeout   = "arangodb-transaction-lockTimeout"
	keyTransactionAllowImplicit = "arangodb-transaction-allowImplicit"
)

func WithTransactionAllowImplicit(
	parent context.Context, value ...bool) context.Context {
	v := true
	if len(value) > 0 {
		v = value[0]
	}
	return context.WithValue(
		contextOrBackground(parent), keyTransactionAllowImplicit, v)
}

func WithTransactionWaitForSync(
	parent context.Context, value ...bool) context.Context {
	v := true
	if len(value) > 0 {
		v = value[0]
	}
	return context.WithValue(
		contextOrBackground(parent), keyTransactionWaitForSync, v)
}

func WithTransactionLockTimeout(
	parent context.Context, value int) context.Context {
	return context.WithValue(
		contextOrBackground(parent), keyTransactionLockTimeout, value)
}

type transaction struct {
	db          *database
	Collections transactionCollections `json:"collections,omitempty"`
	Action      string                 `json:"action,omitempty"`
	WaitForSync bool                   `json:"waitForSync,omitempty"`
	LockTimeout int                    `json:"lockTimeout,omitempty"`
	Params      map[string]interface{} `json:"params,omitempty"`
}

type transactionCollections struct {
	Read          []string `json:"read,omitempty"`
	Write         []string `json:"write,omitempty"`
	AllowImplicit bool     `json:"allowImplicit,omitempty"`
}

type transactionData struct {
	Result *RawObject `json:"result"`
}

func newTransaction(db *database, read []string, write []string) *transaction {
	return &transaction{
		db: db,
		Collections: transactionCollections{
			Read:  read,
			Write: write,
		},
	}
}

var _ Transaction = &transaction{}

func (tx *transaction) relPath() string {
	return path.Join(tx.db.relPath(), "_api", "transaction")
}

func (tx transaction) Execute(ctx context.Context, result interface{}) error {
	var paramKeys []string
	for k := range tx.Params {
		paramKeys = append(paramKeys, k)
	}
	tx.Action = fmt.Sprintf(
		"function(%s){ %s }", strings.Join(paramKeys, ","), tx.Action)
	req, err := tx.db.conn.NewRequest("POST", tx.relPath())
	if err != nil {
		return WithStack(err)
	}
	tx.applyContextSettings(ctx)
	if _, err := req.SetBody(tx); err != nil {
		return WithStack(err)
	}
	resp, err := tx.db.conn.Do(ctx, req)
	if err != nil {
		return WithStack(err)
	}
	if err := resp.CheckStatus(201); err != nil {
		return WithStack(err)
	}
	var data transactionData
	if err := resp.ParseBody("", &data); err != nil {
		return WithStack(err)
	}
	if err := tx.db.conn.Unmarshal(*data.Result, result); err != nil {
		return WithStack(err)
	}
	return nil
}

func (tx *transaction) AddAQL(query string, params map[string]interface{}) error {
	tx.Action += fmt.Sprintf("db.query(%q);", query)
	for key, value := range params {
		tx.Params[key] = value
	}
	return nil
}

func (tx *transaction) AddJSQuery(
	js string, params map[string]interface{}) error {
	tx.Action += js
	for key, value := range params {
		tx.Params[key] = value
	}
	return nil
}

// applyContextSettings fills fields in the queryRequest from the given context.
func (tx *transaction) applyContextSettings(ctx context.Context) {
	if ctx == nil {
		return
	}
	if rawValue := ctx.Value(keyTransactionWaitForSync); rawValue != nil {
		if value, ok := rawValue.(bool); ok {
			tx.Collections.AllowImplicit = value
		}
	}
	if rawValue := ctx.Value(keyTransactionWaitForSync); rawValue != nil {
		if value, ok := rawValue.(bool); ok {
			tx.WaitForSync = value
		}
	}
	if rawValue := ctx.Value(keyTransactionLockTimeout); rawValue != nil {
		if value, ok := rawValue.(int); ok {
			tx.LockTimeout = value
		}
	}
}
