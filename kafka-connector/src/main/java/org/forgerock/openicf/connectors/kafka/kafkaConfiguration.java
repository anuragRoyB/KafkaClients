/*
 * Copyright 2016-2017 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */
package org.forgerock.openicf.connectors.kafka;

import org.identityconnectors.common.Assertions;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;


/**
 * Extends the {@link AbstractConfiguration} class to provide all the necessary
 * parameters to initialize the kafka Connector.
 *
 */
public class kafkaConfiguration extends AbstractConfiguration {


    // Exposed configuration properties.

    /**
     * The connector to connect to.
     */
    private String host;

    /**
     * The Remote user to authenticate with.
     */
    private GuardedString remoteUser = null;

    /**
     * The Password to authenticate with.
     */
    private GuardedString password = null;


    private String clientId;

    private String groupId;

    private String topic;

    


    /**
     * Constructor.
     */
    public kafkaConfiguration() {

    }


    @ConfigurationProperty(order = 1, displayMessageKey = "host.display",
            groupMessageKey = "basic.group", helpMessageKey = "host.help",
            required = true, confidential = false)
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    @ConfigurationProperty(order = 2, displayMessageKey = "remoteUser.display",
            groupMessageKey = "basic.group", helpMessageKey = "remoteUser.help",
            required = true, confidential = false)
    public GuardedString getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(GuardedString remoteUser) {
        this.remoteUser = remoteUser;
    }

    @ConfigurationProperty(order = 3, displayMessageKey = "password.display",
            groupMessageKey = "basic.group", helpMessageKey = "password.help",
            confidential = true)
    public GuardedString getPassword() {
        return password;
    }

    public void setPassword(GuardedString password) {
        this.password = password;
    }


    @ConfigurationProperty(order = 4, displayMessageKey = "clientId.display",
            groupMessageKey = "basic.group", helpMessageKey = "clientId.help",
            required = true, confidential = false)
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    @ConfigurationProperty(order = 5, displayMessageKey = "groupId.display",
            groupMessageKey = "basic.group", helpMessageKey = "groupId.help",
            required = true, confidential = false)
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }



    @ConfigurationProperty(order = 6, displayMessageKey = "topic.display",
            groupMessageKey = "basic.group", helpMessageKey = "topic.help",
            required = true, confidential = false)
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }




    /**
     * {@inheritDoc}
     */
    public void validate() {
        if (StringUtil.isBlank(host)) {
            throw new IllegalArgumentException("Host cannot be null or empty.");
        }

        Assertions.nullCheck(remoteUser, "remoteUser");

        Assertions.nullCheck(password, "password");
    }


}
