package org.opennms.timeseries.cortex.tagstorage;

import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TagStoreUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LokiTagStore.class);

    public static String generateCacheKey(String metricKey, String tenantID) {
        if (tenantID != null && tenantID.trim().length() > 0) {
            return tenantID + "|" + metricKey; // TODO: verify the '|' is not a valid char for the tenant ID
        } else {
            return metricKey; // No tenanting
        }
    }

    public static String combineTagsToString(Set<Tag> tags, String previousTagString) {
        // First get the existing tags, then overwrite with the new ones
        Map<String, String> tagValueMap = new HashMap();

        if (previousTagString != null && !previousTagString.isEmpty()) {
            String[] tagPairs = previousTagString.split("\\|");
            for (int i = 0; i < tagPairs.length; i++) {
                String tagPair = tagPairs[i];
                String[] separatedTag = tagPair.split("=", 2);
                if (separatedTag.length != 2) {
                    LOG.error("Error parsing tags: " + tagPair);
                } else {
                    tagValueMap.put(separatedTag[0], separatedTag[1]);
                }
            }
        }

        StringBuffer log = new StringBuffer();
        boolean needSeparator = false;
        for (Iterator<Tag> iterator = tags.iterator(); iterator.hasNext(); ) {
            Tag tag = iterator.next();
            String key = tag.getKey();
            String value = tag.getValue();

            // Remove from old tags
            tagValueMap.remove(key);

            if (needSeparator) {
                log.append('|');
            } else {
                needSeparator = true;
            }

            log.append(key);
            log.append('=');
            log.append(value);
        }

        // Now append any old tags that weren't updated
        for (Iterator<String> iterator = tagValueMap.keySet().iterator(); iterator.hasNext(); ) {
            String nextKey =  iterator.next();

            if (needSeparator) {
                log.append('|');
            } else {
                needSeparator = true;
            }

            log.append(nextKey);
            log.append('=');
            log.append(tagValueMap.get(nextKey));
        }

        return log.toString();
    }

    public static Metric insertStringTagsInMetric(Metric originalMetric, String encodedTags) {
        if (encodedTags == null || encodedTags.isEmpty()) {
            return originalMetric;
        }

        String[] tagPairs = encodedTags.split("\\|");
        Set<Tag> tags = originalMetric.getExternalTags();

        ImmutableMetric.MetricBuilder metricBuilder = ImmutableMetric.builder();
        metricBuilder.intrinsicTags(originalMetric.getIntrinsicTags());
        metricBuilder.metaTags(originalMetric.getMetaTags());

        for (int i = 0; i < tagPairs.length; i++) {
            String tagString = tagPairs[i];
            String[] separatedTag = tagString.split("=", 2);
            if (separatedTag.length != 2) {
                LOG.error("Error parsing tags: " + tagString);
            } else {
                metricBuilder.externalTag(separatedTag[0], separatedTag[1]);
            }
        }
        return metricBuilder.build();
    }

}
