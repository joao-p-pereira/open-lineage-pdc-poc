package com.pentaho.di.trans.config;

import com.pentaho.di.trans.producer.OpenLineageSenderMode;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A single point of access for all Open Lineage configuration properties.
 */
public class OpenLineageConfig {

  protected static final String MODES_SEPARATOR = ";";

  //Default values
  private final Boolean openLineageActive;
  private final List<OpenLineageSenderMode> openLineageMode;
  private final String openLineageDestinationURL;

  private static OpenLineageConfig instance;

  public static OpenLineageConfig getInstance() {
    if ( null == instance ) {
      instance = new OpenLineageConfig();
    }
    return instance;
  }

  OpenLineageConfig() {
    openLineageActive = Boolean.getBoolean( OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_ACTIVE.getConfigPropName() );
    openLineageMode = getLineageModes();
    openLineageDestinationURL =
      System.getProperty( OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_DESTINATION_URL.getConfigPropName(),
        StringUtils.EMPTY );
  }

  private static List<OpenLineageSenderMode> getLineageModes() {
    List<OpenLineageSenderMode> result = new ArrayList<>();
    String config =
      System.getProperty( OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_MODE.getConfigPropName(),
        OpenLineageSenderMode.CONSOLE.getMode() );
    for ( String mode : config.split( MODES_SEPARATOR ) ) {
      result.add( OpenLineageSenderMode.valueOf( mode.toUpperCase() ) );
    }
    return result;
  }


  public Boolean getOpenLineageActive() {
    return openLineageActive;
  }

  public List<OpenLineageSenderMode> getOpenLineageModes() {
    return openLineageMode;
  }

  public String getOpenLineageDestinationURL() {
    return openLineageDestinationURL;
  }

}
