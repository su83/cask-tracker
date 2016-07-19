/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.tracker.utils;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Common implementation of methods to interact with metadata over HTTP.
 */
public abstract  class AbstractMetaDataClient {
  private static final Type SET_METADATA_SEARCH_RESULT_TYPE =
    new TypeToken<Set<MetadataSearchResultRecord>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec()).create();

  protected abstract  HttpResponse execute(HttpRequest request,  int... allowedErrorCodes)
    throws IOException, UnauthenticatedException;

  protected abstract URL resolve(Id.Namespace namesapace, String resource) throws IOException;


  /**
   * Searches entities in the specified namespace whose metadata matches the specified query.
   *
   * @param namespace the namespace to search in
   * @param query the query string with which to search
   * @param targets {@link MetadataSearchTargetType}s to search. If empty, all possible types will be searched
   * @return A set of {@link MetadataSearchResultRecord} for the given query.
   */
  public Set<MetadataSearchResultRecord> searchMetadata(Id.Namespace namespace, String query,
                                                        Set<MetadataSearchTargetType> targets)
    throws IOException, UnauthenticatedException {

    String path = String.format("metadata/search?query=%s", query);
    for (MetadataSearchTargetType t : targets) {
      path += "&target=" + t;
    }
    URL searchURL = resolve(namespace, path);
    HttpResponse response = execute(HttpRequest.get(searchURL).build());
    return GSON.fromJson(response.getResponseBodyAsString(), SET_METADATA_SEARCH_RESULT_TYPE);
  }


  /**
   * @param id the entity for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If null, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the entity.
   */
  public Set<String> getTags(Id id, @Nullable MetadataScope scope)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException {

    if (id instanceof Id.Application) {
      return getTags((Id.Application) id, scope);
    } else if (id instanceof Id.Artifact) {
      return getTags((Id.Artifact) id, scope);
    } else if (id instanceof Id.DatasetInstance) {
      return getTags((Id.DatasetInstance) id, scope);
    } else if (id instanceof Id.Stream) {
      return getTags((Id.Stream) id, scope);
    } else if (id instanceof Id.Stream.View) {
      return getTags((Id.Stream.View) id, scope);
    } else if (id instanceof Id.Program) {
      return getTags((Id.Program) id, scope);
    }

    throw new IllegalArgumentException("Unsupported Id type: " + id.getClass().getName());
  }

  /**
   * @param appId the app for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the application.
   */
  public Set<String> getTags(Id.Application appId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(appId, constructPath(appId), scope);
  }

  /**
   * @param artifactId the artifact for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the artifact.
   */
  public Set<String> getTags(Id.Artifact artifactId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(artifactId, constructPath(artifactId), scope);
  }

  /**
   * @param datasetInstance the dataset for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the dataset.
   */
  public Set<String> getTags(Id.DatasetInstance datasetInstance, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(datasetInstance, constructPath(datasetInstance), scope);
  }

  /**
   * @param streamId the stream for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the stream.
   */
  public Set<String> getTags(Id.Stream streamId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(streamId, constructPath(streamId), scope);
  }

  /**
   * @param viewId the view for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the view.
   */
  public Set<String> getTags(Id.Stream.View viewId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(viewId, constructPath(viewId), scope);
  }

  /**
   * @param programId the program for which to retrieve metadata tags
   * @param scope the {@link MetadataScope} to retrieve the tags from. If unspecified, this method retrieves
   *              tags from both {@link MetadataScope#SYSTEM} and {@link MetadataScope#USER}
   * @return The metadata tags for the program.
   */
  public Set<String> getTags(Id.Program programId, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    return getTags(programId, constructPath(programId), scope);
  }

  private Set<String> getTags(Id.NamespacedId namespacedId, String entityPath, @Nullable MetadataScope scope)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    String path = String.format("%s/metadata/tags", entityPath);
    path = scope == null ? path : String.format("%s?scope=%s", path, scope);
    HttpResponse response = makeRequest(namespacedId, path, HttpMethod.GET);
    return GSON.fromJson(response.getResponseBodyAsString(), SET_STRING_TYPE);
  }

  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path, HttpMethod httpMethod)
    throws NotFoundException, BadRequestException, UnauthenticatedException, IOException {
    return makeRequest(namespacedId, path, httpMethod, null);
  }

  // makes a request and throws BadRequestException or NotFoundException, as appropriate
  private HttpResponse makeRequest(Id.NamespacedId namespacedId, String path,
                                   HttpMethod httpMethod, @Nullable String body)
    throws IOException, UnauthenticatedException, NotFoundException, BadRequestException {
    URL url = resolve(namespacedId.getNamespace(), path);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url);
    if (body != null) {
      builder.withBody(body);
    }
    HttpResponse response = execute(builder.build(),
                                    HttpURLConnection.HTTP_BAD_REQUEST, HttpURLConnection.HTTP_NOT_FOUND,
                                    HttpURLConnection.HTTP_NOT_AUTHORITATIVE, HttpURLConnection.HTTP_OK);
    if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(response.getResponseBodyAsString());
    }
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException(namespacedId);
    }
    return response;
  }

  // construct a component of the path, specific to each entity type
  private String constructPath(Id.Application appId) {
    return String.format("apps/%s", appId.getId());
  }

  private String constructPath(Id.Artifact artifactId) {
    return String.format("artifacts/%s/versions/%s", artifactId.getName(), artifactId.getVersion().getVersion());
  }

  private String constructPath(Id.Program programId) {
    return String.format("apps/%s/%s/%s",
                         programId.getApplicationId(), programId.getType().getCategoryName(), programId.getId());
  }

  private String constructPath(Id.DatasetInstance datasetInstance) {
    return String.format("datasets/%s", datasetInstance.getId());
  }

  private String constructPath(Id.Stream streamId) {
    return String.format("streams/%s", streamId.getId());
  }

  private String constructPath(Id.Stream.View viewId) {
    return String.format("streams/%s/views/%s", viewId.getStreamId(), viewId.getId());
  }
}
