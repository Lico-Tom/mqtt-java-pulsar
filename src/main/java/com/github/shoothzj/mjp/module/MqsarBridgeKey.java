package com.github.shoothzj.mjp.module;

import io.netty.channel.ChannelHandlerContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;

@Setter
@Getter
@EqualsAndHashCode
public class MqsarBridgeKey {

    @NotNull
    private ChannelHandlerContext ctx;

    @NotNull
    private String topic;

    public MqsarBridgeKey() {

    }
}
