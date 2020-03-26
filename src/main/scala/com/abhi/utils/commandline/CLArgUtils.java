package com.abhi.utils.commandline;

import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class CLArgUtils {
  private PrintStream out;
  Properties properties = new Properties();
  Map<String, String> argsMap = new LinkedHashMap<String, String>();
  public CLArgUtils(PrintStream out) {
    this.out = out;
  }

  public Properties process(String[] args) {
    Properties properties = new Properties();
    if(args == null || args.length<2 || args.length%2 !=0)
      return properties;
    for(int i=0; i< args.length; i=i+2){
      String param;
      if(args[i].startsWith("--")){
        param = args[i].substring(2);
      }
      else if(args[i].startsWith("-")){
        param = args[i].substring(1);
      }
      else{
        out.printf("Failed to process %s", args[i]);
        continue;
      }
      properties.setProperty(param, args[i+1]);
    }
    return properties;
  }
}
