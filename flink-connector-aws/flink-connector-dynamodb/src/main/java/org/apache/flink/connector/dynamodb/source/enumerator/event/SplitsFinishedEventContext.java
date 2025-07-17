package org.apache.flink.connector.dynamodb.source.enumerator.event;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.services.dynamodb.model.Shard;

import java.io.Serializable;
import java.util.List;

/** Context which contains the split id and the finished splits for a finished split event. */
@Internal
public class SplitsFinishedEventContext implements Serializable {
    String splitId;
    List<Shard> childSplits;

    public SplitsFinishedEventContext(String splitId, List<Shard> childSplits) {
        this.splitId = splitId;
        this.childSplits = childSplits;
    }

    public String getSplitId() {
        return splitId;
    }

    public List<Shard> getChildSplits() {
        return childSplits;
    }
}
