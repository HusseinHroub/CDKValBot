package com.myorg;

import com.myorg.common.Constants;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

public class CdkValBotApp {

    public static void main(final String[] args) {
        App app = new App();

        new CdkValBotStack(app, "CdkValBotStackProd", StackProps.builder()
                .env(Environment.builder()
                        .account(Constants.AWS_ACCOUNT_ID)
                        .region(Constants.PROD_REGION)
                        .build())
                .build());

//        new CdkValBotStack(app, "CdkValBotStackDevo", StackProps.builder()
//                .env(Environment.builder()
//                        .account(Constants.AWS_ACCOUNT_ID)
//                        .region(Constants.DEVO_REGION)
//                        .build())
//                .build());

        app.synth();
    }
}

