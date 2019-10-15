package com.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CmdLineCommon {

    private static final String IP_KEY = "a";
    private String ip;

    private static final String PORT_KEY = "p";
    private int port;
    private static final int DEFAULT_PORT = 1919;
    private static final String THROWAWAY_BUFFERS="b";
    private int tb=0;

    private final String appName;

    private final Options options;

    public CmdLineCommon(String appName) {
        this.appName = appName;

        this.options = new Options();
        Option address = Option.builder(IP_KEY).required().desc("ip address").hasArg().required().build();
        Option port = Option.builder(PORT_KEY).desc("port").hasArg().type(Number.class).build();
        Option buffers = Option.builder(THROWAWAY_BUFFERS).desc("throw away buffers-- for MR benchmark").hasArg().type(Number.class).build();
        options.addOption(address);
        options.addOption(port);
        options.addOption(buffers);
    }

    protected Options addOption(Option option) {
        return options.addOption(option);
    }

    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(appName, options);
    }

    protected void getOptionsValue(CommandLine line) throws ParseException {
        ip = line.getOptionValue(IP_KEY);
        if (line.hasOption(PORT_KEY)) {
            port = ((Number) line.getParsedOptionValue(PORT_KEY)).intValue();
        } else {
            port = DEFAULT_PORT;
        }

        if (line.hasOption(THROWAWAY_BUFFERS)) {
            tb = ((Number) line.getParsedOptionValue(THROWAWAY_BUFFERS)).intValue();
        }
    }

    public void parse(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);
        getOptionsValue(line);
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public int getThrowAwayBufferCount(){
        return tb;
    }
}
