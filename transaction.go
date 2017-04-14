//
// DISCLAIMER
//
// Copyright 2017 Simeon Mugisha, Dar es Salaam, Tanzania
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
// Copyright holder is Simeon Mugisha
//

package driver

import "context"

// Transaction is
type Transaction interface {
	AddJSQuery(js string, bindVars map[string]interface{}) error
	AddAQL(query string, bindVars map[string]interface{}) error
	Execute(ctx context.Context, result interface{}) error
}
