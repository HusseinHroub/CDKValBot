package com.myorg;

import com.myorg.constrcuts.AsyncComputeConstructs;
import com.myorg.constrcuts.StorageConstruct;
import com.myorg.constrcuts.SyncComputeConstructs;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.constructs.Construct;

public class CdkValBotStack extends Stack {
    public CdkValBotStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public CdkValBotStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);
        StorageConstruct storageConstruct = new StorageConstruct(this);
        new SyncComputeConstructs(this, storageConstruct);
        new AsyncComputeConstructs(this, storageConstruct);
    }
}
