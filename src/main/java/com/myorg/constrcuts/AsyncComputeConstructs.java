package com.myorg.constrcuts;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleProps;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSource;
import software.amazon.awscdk.services.lambda.eventsources.SqsEventSourceProps;
import software.amazon.awscdk.services.sqs.Queue;
import software.constructs.Construct;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static com.myorg.common.ConstructBuilderUtils.getQueueWithDefaultSettingsAndDLQ;

public class AsyncComputeConstructs extends Construct {
    public AsyncComputeConstructs(final Construct scope, final StorageConstruct storageConstruct) {
        super(scope, "AsyncComputeConstruct");

        File lambdasDir = new File(System.getProperty("user.dir") + "/" + "lambdas");


        Queue playersDynamoTrackQueue = getQueueWithDefaultSettingsAndDLQ(this, "PlayersDynamoTrack");
        Queue playerMatchesQueue = getQueueWithDefaultSettingsAndDLQ(this, "PlayerMatches.fifo");
        Queue playerMatchDetailsQueue = getQueueWithDefaultSettingsAndDLQ(this, "PlayerMatchDetails.fifo");
        Queue playerStatChangesQueue = getQueueWithDefaultSettingsAndDLQ(this, "PlayerStatChanges");
        Rule monthlyRuleSchedule = getMonthlyRuleSchedule();

        createPlayersTracerIngestetor(lambdasDir, playersDynamoTrackQueue, storageConstruct.getPlayerRefTable());
        createPlayerTrackHandler(lambdasDir, playersDynamoTrackQueue, playerMatchesQueue, storageConstruct.getPlayerRefTable());
        createPlayerMatchTrackHandler(lambdasDir, playerMatchesQueue, playerMatchDetailsQueue);
        createPlayerMatchInfoHandler(lambdasDir, playerMatchDetailsQueue, playerStatChangesQueue, storageConstruct.getPlayerDataTrackingTable(), storageConstruct.getPlayerRefTable());
        createDiscordPlayerChangesNotifier(lambdasDir, playerStatChangesQueue);
        createAnnouncerLambdaOfEachMonth(lambdasDir, monthlyRuleSchedule, storageConstruct.getPlayerDataTrackingTable());
    }

    @NotNull
    private Rule getMonthlyRuleSchedule() {
        return new Rule(this, "scheduleRule", new RuleProps() {
            @Override
            public Schedule getSchedule() {
                return Schedule.expression("cron(0 8 1 * ? *)");//8 AM on day 1 of each month
            }
        });
    }

    private void createPlayersTracerIngestetor(File lambdasDir, Queue queue, Table dynamoTable) {
        Function function = Function.Builder.create(this, "PlayersTrackerIngestor")
                .code(Code.fromAsset(new File(lambdasDir, "player_tracker_ingestor").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("PlayersTrackerIngestor")
                .description("Scan main dynamo table for the specified partition, and ingest in Queue")
                .environment(Map.of("queueURL", queue.getQueueUrl(), "dynamoTableName", dynamoTable.getTableName()))
                .timeout(Duration.minutes(15))
                .handler("index.lambda_handler").build();
        queue.grantSendMessages(function);
        dynamoTable.grantFullAccess(function);
    }


    private void createPlayerTrackHandler(File lambdasDir, Queue sourceQueue, Queue playerMatchQueue, Table dynamoTable) {
        Function function = Function.Builder.create(this, "PlayerTrackHandler")
                .code(Code.fromAsset(new File(lambdasDir, "player_dynamo_track_handler").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("PlayerTrackHandler")
                .description("Handle player events, this will trigger RIOT API to get this player match id details")
                .events(Collections.singletonList(new SqsEventSource(sourceQueue, new SqsEventSourceProps() {
                    @Override
                    public @Nullable Number getBatchSize() {
                        return 1;
                    }
                })))
                .environment(Map.of("playerMatchQueueURL", playerMatchQueue.getQueueUrl(), "dynamoTableName", dynamoTable.getTableName()))
                .timeout(Duration.minutes(3))
                .handler("index.lambda_handler").build();
        playerMatchQueue.grantSendMessages(function);
        dynamoTable.grantFullAccess(function);
    }

    private void createPlayerMatchTrackHandler(File lambdasDir, Queue sourceQueue, Queue playerMatchInfoQueue) {
        Function function = Function.Builder.create(this, "PlayerMatchTrackHandler")
                .code(Code.fromAsset(new File(lambdasDir, "player_match_track_handler").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("PlayerMatchTrackHandler")
                .description("Handle an event related to new match id, this function will trigger RIOT api to get full match info")
                .events(Collections.singletonList(new SqsEventSource(sourceQueue, new SqsEventSourceProps() {
                    @Override
                    public @Nullable Number getBatchSize() {
                        return 1;
                    }
                })))
                .environment(Map.of("playerMatchInfoQueueUrl", playerMatchInfoQueue.getQueueUrl()))
                .timeout(Duration.minutes(3))
                .handler("index.lambda_handler").build();
        playerMatchInfoQueue.grantSendMessages(function);
    }

    private void createPlayerMatchInfoHandler(File lambdasDir, Queue sourceQueue, Queue playerChangesQueue, Table playerDataTrackTable, Table playerRefTable) {
        Function function = Function.Builder.create(this, "PlayerMatchInfoHandler")
                .code(Code.fromAsset(new File(lambdasDir, "player_match_tracking_info_handler").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("PlayerMatchInfoHandler")
                .description("Handle player match info events")
                .events(Collections.singletonList(new SqsEventSource(sourceQueue)))
                .environment(Map.of("playerChangesQueueURL", playerChangesQueue.getQueueUrl(), "playerDataTrackTableName", playerDataTrackTable.getTableName(), "playerRefTableName", playerRefTable.getTableName()))
                .timeout(Duration.minutes(3))
                .handler("index.lambda_handler").build();
        playerChangesQueue.grantSendMessages(function);
        playerDataTrackTable.grantFullAccess(function);
        playerRefTable.grantFullAccess(function);
    }


    private void createDiscordPlayerChangesNotifier(File lambdasDir, Queue playerStatChangesQueue) {
        Function.Builder.create(this, "DiscordPlayerChangesNotifier")
                .code(Code.fromAsset(new File(lambdasDir, "discord_player_changes_notifier").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("DiscordPlayerChangesNotifier")
                .description("Handle player new data changes, and announce to discord channel if something special")
                .events(Collections.singletonList(new SqsEventSource(playerStatChangesQueue)))
                .timeout(Duration.minutes(3))
                .handler("index.lambda_handler").build();
    }

    private void createAnnouncerLambdaOfEachMonth(File lambdasDir, Rule monthlyRuleSchedule, Table playerDataTrackTable) {
        Function function = Function.Builder.create(this, "AnnouncerStats")
                .code(Code.fromAsset(new File(lambdasDir, "stats_announcer").toString()))
                .runtime(Runtime.PYTHON_3_7)
                .functionName("AnnouncerStats")
                .description("Shall be triggered in monthly bases to announce top players on each metric")
                .timeout(Duration.minutes(3))
                .environment(Map.of("playerDataTrackingTableName", playerDataTrackTable.getTableName()))
                .handler("index.lambda_handler").build();
        monthlyRuleSchedule.addTarget(new LambdaFunction(function));
        playerDataTrackTable.grantReadData(function);
    }

}
