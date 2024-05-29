package com.pentaho.di.trans.producer;

public enum OpenLineageSenderMode {


  /**
   * Console
   */
  CONSOLE("CONSOLE"),

  /**
   * HTTP
   */
  HTTP("HTTP");


  private final String mode;

  OpenLineageSenderMode( String mode) {
    this.mode = mode;
  }

  public String getMode() {
    return mode;
  }
}
