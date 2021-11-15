/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.connector.ssl

case class CASSLSignParams(caCrtFilePath: String, crtFilePath: String, keyFilePath: String)

case class SelfSSLSignParams(crtFilePath: String, keyFilePath: String, password: String)
