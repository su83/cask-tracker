package co.cask.tracker;

import co.cask.cdap.test.ServiceManager;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class containing common code for tests
 */
public class TestUtils {
  public static String getServiceResponse(ServiceManager serviceManager, String request, String type,
                                          String postRequest, int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(type);
    List<Integer> expectedCodes = new ArrayList<>();
    expectedCodes.add(HttpURLConnection.HTTP_BAD_REQUEST);
    expectedCodes.add(HttpURLConnection.HTTP_CONFLICT);
    expectedCodes.add(HttpURLConnection.HTTP_NOT_FOUND);
    String response;
    try {
      //Feed JSON data if POST
      if (type.equals("POST") || type.equals("PUT")) {
        connection.setDoOutput(true);
        connection.getOutputStream().write(postRequest.getBytes());
      }
      Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (expectedCodes.contains(connection.getResponseCode())) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }

  // Request is GET by default
  public static String getServiceResponse(ServiceManager serviceManager, String request, String type,
                                          int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod(type);
    Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }

  public static String getServiceResponse(ServiceManager serviceManager,
                                          String request,
                                          int expectedResponseCode) throws Exception {
    URL url = new URL(serviceManager.getServiceURL(), request);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(expectedResponseCode, connection.getResponseCode());
    String response;
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
      } else if (connection.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
        response = new String(ByteStreams.toByteArray(connection.getErrorStream()), Charsets.UTF_8);
      } else {
        throw new Exception("Invalid response code returned: " + connection.getResponseCode());
      }
    } finally {
      connection.disconnect();
    }
    return response;
  }
}
