package com.pentaho.di.trans.config;

/**
 * This enum is used to define a number of configurations for various settings of OpenLineage.
 */
public enum OpenLineageConfigNames {

  /**
   * Sets open lineage active when set to true.
   */
  KETTLE_OPEN_LINEAGE_ACTIVE( "KETTLE_OPEN_LINEAGE_ACTIVE" ),

  /**
   * Sets the way open lineage will be displayed/pushed.
   * Currently supported HTTP, Console (Spoon log)
   */
  KETTLE_OPEN_LINEAGE_MODE( "KETTLE_OPEN_LINEAGE_MODE" ),

  /**
   * When HTTP mode is active this will be the destination URL for the open lineage events.
   */
  KETTLE_OPEN_LINEAGE_DESTINATION_URL( "KETTLE_OPEN_LINEAGE_DESTINATION_URL" );

  private final String configPropName;

  OpenLineageConfigNames( String configName ) {
    this.configPropName = configName;
  }

  public String getConfigPropName() {
    return configPropName;
  }
}