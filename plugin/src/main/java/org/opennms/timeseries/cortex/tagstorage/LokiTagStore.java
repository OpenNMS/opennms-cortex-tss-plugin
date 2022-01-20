package org.opennms.timeseries.cortex.tagstorage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.timeseries.cortex.CortexTSSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.opennms.timeseries.cortex.tagstorage.TagStoreUtils.*;

public class LokiTagStore implements TagStorage {
    private static final Logger LOG = LoggerFactory.getLogger(LokiTagStore.class);

    private static final String X_SCOPE_ORG_ID_HEADER = "X-Scope-OrgID";
    private static final long THIRTY_DAYS_MILLIS = (1000 * 60 * 60 * 24 * 30);
    private static final String LOKI_START_TIME_HEADER = "start";
    private static final String LOKI_END_TIME_HEADER = "end";

    private String lokiReadUrl;
    private String lokiWriteUrl;
    private OkHttpClient client;

    private final boolean enableCache = false;
    private Cache<String, String> tagCache;

    public LokiTagStore(CortexTSSConfig config) {
        // todo: Add loki config to the CortexTSSConfig
        String host = config.getExternalTagStorageHost();
        long port = config.getExternalTagStoragePort();
        lokiReadUrl = "http://" + host + ":" + port + "/loki/api/v1/query_range";
        lokiWriteUrl = "http://" + host + ":" + port + "/loki/api/v1/push";

        ConnectionPool connectionPool = new ConnectionPool(config.getMaxConcurrentTagStorageConnections(), 5, TimeUnit.MINUTES);
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(config.getMaxConcurrentTagStorageConnections());
        dispatcher.setMaxRequestsPerHost(config.getMaxConcurrentTagStorageConnections());

        tagCache = CacheBuilder.newBuilder()
                .maximumSize(config.getMaxTagCacheSize())
                .expireAfterWrite(10, TimeUnit.DAYS)
                .build();

        client = new OkHttpClient.Builder()
                .readTimeout(config.getReadTimeoutInMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(config.getWriteTimeoutInMs(), TimeUnit.MILLISECONDS)
                .dispatcher(dispatcher)
                .connectionPool(connectionPool)
                .build();
    }

    @Override
    public void storeTags(Sample sample, String tenantID) {
        String key = sample.getMetric().getKey();;

        // Get old value first
        String oldLog = retrieveStringLog(sample.getMetric(), tenantID, sample.getTime().getEpochSecond() * 1000);

        String tagLog = combineTagsToString(sample.getMetric().getExternalTags(), oldLog);

        String cacheEntry = tagCache.getIfPresent(generateCacheKey(key, tenantID));

        if ((cacheEntry != null) && (cacheEntry.equals(tagLog))) {
            return;
        }

        // We don't correlate times with the metrics. We only ever look for the latest tag from here,
        // so use the current time for the timestamp
        long timemillis = sample.getTime().toEpochMilli();
        String jsonpost = "{ " +
                "\"streams\": [ " +
                "{ " +
                "\"stream\": { " +
                "\"key\": \"" + key + "\"" +
                "}, " +
                "\"values\": [ [ " +
                "\"" + timemillis + "000000\"," +  // Converting millis to nanos
                "\"" + tagLog + "\"" +
                "] ] " +
                "} " +
                "] " +
                "}";

        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json"), jsonpost);
        Request.Builder builder = new Request.Builder()
                .url(lokiWriteUrl)
                .header("Content-Type", "application/json")  // Todo: is this line necessary?
                .post(requestBody);

        // Add the OrgId header if set
        if (tenantID != null && tenantID.trim().length() > 0) {
            builder.addHeader(X_SCOPE_ORG_ID_HEADER, tenantID);
        }

        Request request = builder.build();

        try (Response response = client.newCall(request).execute()) {
            ResponseBody responseBody = response.body();
            if (!response.isSuccessful()) {
                String bodyMsg = "";
                if (responseBody != null) {
                    bodyMsg = responseBody.string();
                }
                LOG.error(String.format("Call to %s failed: response code:%s, response message:%s, bodyMessage:%s", lokiWriteUrl, response.code(), response.message(), bodyMsg));
                return;
            }
            if (enableCache) {
                tagCache.put(generateCacheKey(sample.getMetric().getKey(), tenantID), tagLog);
            }
        } catch (IOException e) {
            LOG.error(String.format("Call to %s failed.", lokiWriteUrl), e);
        }
    }

    @Override
    public Metric retrieveTags(Metric metric, String tenantID) {
        final long endTimeEpochMillis = System.currentTimeMillis();
        String stringLog = retrieveStringLog(metric, tenantID, endTimeEpochMillis);

        if (stringLog != null && !stringLog.isEmpty()) {
            return convertEncodedStringToTags(metric, stringLog);
        }

        // Failure to get the loki tags, return what we started with
        return metric;
    }

    private String retrieveStringLog(Metric metric, String tenantID, long endTimeEpochMillis) {
        // Get from the cache if it's in there
        String cacheEntry = tagCache.getIfPresent(generateCacheKey(metric.getKey(), tenantID));

        if (cacheEntry != null) {
            return cacheEntry;
        }

        // Need to get from Loki itself
        HttpUrl.Builder urlBuilder = HttpUrl.parse(lokiReadUrl).newBuilder();
        urlBuilder.addQueryParameter("query", "{key=\"" + metric.getKey() + "\"}");
        urlBuilder.addQueryParameter("limit", "1"); // Only want the latest
        String lokiUrl = urlBuilder.build().toString();

        Request.Builder builder = new Request.Builder()
                .url(lokiUrl)
                .header("Content-Type", "application/json");  // Todo: is this line necessary?

        // Add the OrgId header if set
        if (tenantID != null && tenantID.trim().length() > 0) {
            builder.addHeader(X_SCOPE_ORG_ID_HEADER, tenantID);
        }

        // Set the end time of the query to be that of the metric
        builder.addHeader(LOKI_END_TIME_HEADER, String.valueOf(endTimeEpochMillis) + "0000"); // Millis to nanos
        // Set the start time of the query to 30 days before the metric. Should be entries within that window
        long requestStartTime = endTimeEpochMillis - THIRTY_DAYS_MILLIS;
        builder.addHeader(LOKI_START_TIME_HEADER, String.valueOf(requestStartTime) + "0000"); // Millis to nanos

        Request request = builder.build();

        try (Response response = client.newCall(request).execute()) {
            try(ResponseBody responseBody = response.body()) {
                if (!response.isSuccessful()) {
                    String bodyMsg = "";
                    if (responseBody != null) {
                        bodyMsg = responseBody.string();
                    }
                    throw new StorageException(String.format("Call to %s failed: response code:%s, response message:%s, bodyMessage:%s", lokiUrl, response.code(), response.message(), bodyMsg));
                }

                if (responseBody != null) {
                    return convertLokiJsonToString(responseBody.string());
                } else {
                    LOG.error(String.format("Call to %s delivered no body.", lokiUrl));
                }
            }
        } catch (IOException | StorageException e) {
            LOG.error(String.format("Call to %s failed.", lokiWriteUrl), e);
        }
        return null;
    }

    private String convertLokiJsonToString(String jsonStr) {
        JSONObject json = new JSONObject(jsonStr);
        JSONObject lokiData = json.getJSONObject("data");

        if (lokiData == null) {
            LOG.error("Invalid Loki json - no data");
            return null;
        }
        JSONArray resultArray = lokiData.getJSONArray("result");
        if (resultArray == null || resultArray.length() == 0) {
            LOG.error("Invalid Loki json - no results");
            return null;
        }

        JSONObject firstResult = resultArray.getJSONObject(0);
        if (firstResult == null) {
            LOG.error("Invalid Loki json - empty result");
            return null;
        }

        JSONArray values = firstResult.getJSONArray("values");
        if (values == null || values.length() == 0) {
            LOG.error("Invalid Loki json - no values");
            return null;
        }

        JSONArray valueSet = values.getJSONArray(0); // We should only be getting the first
        if (valueSet == null || valueSet.length() < 2) {
            LOG.error("Invalid Loki json - bad value pair");
            return null;
        }

        // Timestamp is the first. the value is the second
        return valueSet.getString(1);
    }

    private Metric convertEncodedStringToTags(Metric metric, String encodedTags) {
        return insertStringTagsInMetric(metric, encodedTags);
    }

}
