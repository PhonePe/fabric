/*
 * Copyright 2016 ANI Technologies Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.olacabs.fabric.compute.builder.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.jfrog.artifactory.client.Artifactory;
import org.jfrog.artifactory.client.ArtifactoryClient;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Resolves Artifactory URLs for component JARs based on maven groupId, artifactId and version.
 */
@Slf4j
public final class ArtifactoryJarPathResolver {

    private ArtifactoryJarPathResolver() {

    }

    public static String resolve(final String artifactoryUrl, final String groupId, final String artifactId,
            final String version) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(artifactoryUrl), "Artifactory URL cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "Group Id cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(artifactId), "Artifact Id cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(version), "Artifact version cannot be null");
        boolean isSnapshot = version.contains("SNAPSHOT");
        log.info("Artifact is snapshot: {}", isSnapshot);
        final String repoName = isSnapshot ? "libs-snapshot-local" : "libs-release-local";

        Artifactory client = ArtifactoryClient.create(artifactoryUrl);
        log.info("Aritifactory client created successfully with uri {}", client.getUri());
        FileAttribute<Set<PosixFilePermission>> perms =
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"));
        java.nio.file.Path tempFilePath = Files.createTempFile(Long.toString(System.currentTimeMillis()), "xml", perms);
        String metadataStr;
        if (isSnapshot) {
            metadataStr =
                    String.format("%s/%s/%s/maven-metadata.xml", groupId.replaceAll("\\.", "/"), artifactId, version);
        } else {
            metadataStr = String.format("%s/%s/maven-metadata.xml", groupId.replaceAll("\\.", "/"), artifactId);
        }

        log.info("Repo-name - {}, metadataStr - {}", repoName, metadataStr);
        InputStream response = client.repository(repoName).download(metadataStr).doDownload();
        log.info("download complete");
        Files.copy(response, tempFilePath, StandardCopyOption.REPLACE_EXISTING);
        log.info("Metadata file downloaded to: {}", tempFilePath.toAbsolutePath().toString());

        final String url =
                String.format("%s/%s/%s/%s/%s/%s-%s.jar", artifactoryUrl, repoName, groupId.replaceAll("\\.", "/"),
                        artifactId, version, artifactId, version);
        log.info("Jar will be downloaded from: " + url);
        return url;
    }

}
