package com.myorg.constrcuts;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.LayerVersion;
import software.amazon.awscdk.services.lambda.Runtime;
import software.constructs.Construct;

import java.io.File;
import java.util.Collections;
import java.util.Map;

public class SyncComputeConstructs extends Construct {

    public SyncComputeConstructs(final Construct scope, final StorageConstruct storageConstruct) {
        super(scope, "SyncComputeConstruct");

        File lambdasDir = new File(System.getProperty("user.dir") + "/" + "lambdas");

        createRiotRSOLambda(lambdasDir, storageConstruct.getPlayerDataTrackingTable(), storageConstruct.getPlayerRefTable());
        createDiscordCommandHandlerLambda(lambdasDir, storageConstruct.getPlayerDataTrackingTable());
    }


    private void createRiotRSOLambda(File lambdasDir, Table playerTrackingTable, Table playerRefTable) {
        Function function = Function.Builder.create(this, "RiotRSOProcess")
                .code(Code.fromAsset(new File(lambdasDir, "riot_rso_process").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("RiotRSOProcess")
                .description("Register player from RIOT RSO, players will interact with this function when they authorize this app")
                .environment(Map.of("playerTrackingTableName", playerTrackingTable.getTableName(), "playerRefTableName", playerRefTable.getTableName()))
                .timeout(Duration.minutes(3))
                .handler("index.lambda_handler").build();
        playerTrackingTable.grantFullAccess(function);
        playerRefTable.grantFullAccess(function);
    }

    private void createDiscordCommandHandlerLambda(File lambdasDir, Table playerDataTrackingTable) {
        Function function = Function.Builder.create(this, "DiscordCommandHandler")
                .code(Code.fromAsset(new File(lambdasDir, "discord_command_handler").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("DiscordCommandHandler")
                .description("Handle commands from discord channel")
                .environment(Map.of("playerDataTrackingTableName", playerDataTrackingTable.getTableName()))
                .timeout(Duration.minutes(3))
                .layers(Collections.singletonList(LayerVersion.Builder.create(this, "DiscordLambdaLayer")
                        .compatibleRuntimes(Collections.singletonList(Runtime.PYTHON_3_7))
                        .code(Code.fromAsset(new File(lambdasDir, "layers/discord_command_handler").toString()))
                        .description("Contain basic libraries for DiscordCommandHandler")
                        .build()))
                .handler("index.lambda_handler").build();
        playerDataTrackingTable.grantFullAccess(function);


    }

}
