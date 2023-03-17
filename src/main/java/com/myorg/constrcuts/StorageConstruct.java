package com.myorg.constrcuts;

import software.amazon.awscdk.services.dynamodb.*;
import software.constructs.Construct;

import java.util.Collections;

public class StorageConstruct extends Construct {
    private final Table playerDataTrackingTable;
    private final Table playerRefTable;

    public StorageConstruct(final Construct scope) {
        super(scope, "StorageConstruct");
        this.playerDataTrackingTable = createPlayerDataTrackingTable(scope);
        this.playerRefTable = createPlayerRefTable(scope);

    }

    private Table createPlayerDataTrackingTable(Construct scope) {
        Table playerDataTrackingTable = Table.Builder.create(scope, "PlayerDataTracking")
                .billingMode(BillingMode.PROVISIONED)
                .readCapacity(5)
                .writeCapacity(5)
                .tableName("PlayerDataTracking")
                .partitionKey(Attribute.builder().name("puuid").type(AttributeType.STRING).build())
                .sortKey(Attribute.builder().name("month").type(AttributeType.NUMBER).build())
                .build();
        //playerDataTrackingTable.autoScaleReadCapacity TODO check if this is needed after deploy

        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerDataRankGSI("assists"));
        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerDataRankGSI("deaths"));
        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerDataRankGSI("kda"));
        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerDataRankGSI("kills"));
        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerDataRankGSI("winrate"));
        playerDataTrackingTable.addGlobalSecondaryIndex(getPlayerPnameGSI());
        return playerDataTrackingTable;
    }

    private GlobalSecondaryIndexProps getPlayerPnameGSI() {
        final String partitionKey = "pname";
        return GlobalSecondaryIndexProps.builder()
                .indexName(partitionKey + "-" + "month" + "-index")
                .partitionKey(Attribute.builder().name(partitionKey).type(AttributeType.STRING).build())
                .sortKey(Attribute.builder().name("month").type(AttributeType.NUMBER).build())
                .readCapacity(3)
                .writeCapacity(3)
                .projectionType(ProjectionType.ALL)
                .build();
    }


    private GlobalSecondaryIndexProps getPlayerDataRankGSI(final String sortNumberKey) {
        final String partitionKey = "month";
        return GlobalSecondaryIndexProps.builder()
                .indexName(partitionKey + "-" + sortNumberKey + "-index")
                .partitionKey(Attribute.builder().name(partitionKey).type(AttributeType.NUMBER).build())
                .sortKey(Attribute.builder().name(sortNumberKey).type(AttributeType.NUMBER).build())
                .readCapacity(3)
                .writeCapacity(3)
                .projectionType(ProjectionType.INCLUDE)
                .nonKeyAttributes(Collections.singletonList("pname"))
                .build();
    }

    private Table createPlayerRefTable(Construct scope) {
        return Table.Builder.create(scope, "PlayerRef")
                .billingMode(BillingMode.PROVISIONED)
                .readCapacity(5)
                .writeCapacity(5)
                .tableName("PlayerRef")
                .partitionKey(Attribute.builder().name("partn").type(AttributeType.STRING).build())
                .sortKey(Attribute.builder().name("puuid").type(AttributeType.STRING).build())
                .build();
    }

    public Table getPlayerDataTrackingTable() {
        return playerDataTrackingTable;
    }

    public Table getPlayerRefTable() {
        return playerRefTable;
    }
}
