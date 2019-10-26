package dev.shreyas.flink.course.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;

public class InputUtil {
    private ParameterTool parameterTool;

    public InputUtil(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    public String getInput(String parameter){
        if (!parameterTool.has(parameter)){
            throw new RuntimeException("Please provide --"+parameter+" followed with path to file");
        }
        return parameterTool.getRequired(parameter);
    }
}
