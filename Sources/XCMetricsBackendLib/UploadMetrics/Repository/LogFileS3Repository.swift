// Copyright (c) 2020 Spotify AB.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Foundation
import Vapor
import SotoS3
import SotoSTS

/// `LogFileRepository` that uses Amazon S3 to store and fetch logs
struct LogFileS3Repository: LogFileRepository {

    let bucketName: String

    let s3: S3

    init(bucketName: String, regionName: String) {
        self.bucketName = bucketName
        guard let region = Region.init(awsRegionName: regionName) else {
            preconditionFailure("Invalid S3 Region \(regionName)")
        }

        let roleArn = Environment.get("AWS_ROLE_ARN") ?? ""
        let assumeRoleRequest = SotoSTS.STS.AssumeRoleRequest(roleArn: roleArn, roleSessionName: "xcmetrics")
        let assumeRoleCredentialProvider = SotoSTS.CredentialProviderFactory.stsAssumeRole(request: assumeRoleRequest, region: region)

        let webIdentityCredentialProvider = SotoSTS.CredentialProviderFactory.stsWebIdentityTokenFile(region: region)

        let client = AWSClient(credentialProvider: .selector(assumeRoleCredentialProvider, .environment, webIdentityCredentialProvider, .ecs, .ec2, CredentialProviderFactory.configFile()), httpClientProvider: .createNew)

        self.s3 = S3(client: client, region: region)
    }

    init?(config: Configuration) {
        guard let bucketName = config.s3Bucket,
              let regionName = config.s3Region else {
            return nil
        }
        self.init(bucketName: bucketName, regionName: regionName)
    }

    func put(logFile: File) throws -> URL {
        let data = Data(logFile.data.xcm_onlyFileData().readableBytesView)
        let putObjectRequest = S3.PutObjectRequest(acl: .private,
                                                   body: AWSPayload.data(data),
                                                   bucket: bucketName,
                                                   key: logFile.filename)
        let fileURL = try s3.putObject(putObjectRequest)
            .map { _ -> URL? in
                return URL(string: "s3://\(bucketName)/\(logFile.filename)")
            }.wait()
        guard let url = fileURL else {
            throw RepositoryError.unexpected(message: "Invalid url of \(logFile.filename)")
        }
        return url
    }

    func get(logURL: URL) throws -> LogFile {
        guard let bucket = logURL.host else {
            throw RepositoryError.unexpected(message: "URL is not an S3 url \(logURL)")
        }
        let fileName = logURL.lastPathComponent
        let request = S3.GetObjectRequest(bucket: bucket, key: fileName)
        let fileData = try s3.getObject(request).wait().body
        guard let data = fileData?.asData() else {
            throw RepositoryError.unexpected(message: "There was an error downloading file \(logURL)")
        }
        let tmp = try TemporaryFile(creatingTempDirectoryForFilename: "\(UUID().uuidString).xcactivitylog")
        try data.write(to: tmp.fileURL)
        return LogFile(remoteURL: logURL, localURL: tmp.fileURL)
    }

}
