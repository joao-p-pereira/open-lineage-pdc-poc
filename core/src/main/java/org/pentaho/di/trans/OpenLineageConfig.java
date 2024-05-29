package org.pentaho.di.trans;

import java.util.ArrayList;
import java.util.List;

import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_ACTIVE;
import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_DESTINATION_URL;
import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_MODE;

/**
 * A single point of access for all Open Lineage configuration properties.
 */
public class OpenLineageConfig {

  //Default values
  private Boolean openLineageActive = false;
  private List<OpenLineageSenderMode> openLineageMode = new ArrayList<>(List.of( OpenLineageSenderMode.CONSOLE));
  private String openLineageDestinationURL = "";

  private static OpenLineageConfig instance;

  public static OpenLineageConfig getInstance() {
    if ( null == instance ) {
      instance = new OpenLineageConfig();
    }
    return instance;
  }

  OpenLineageConfig() {
    openLineageActive = Boolean.getBoolean( KETTLE_OPEN_LINEAGE_ACTIVE.getConfigPropName() );
    openLineageMode = getLineageModes();
    openLineageDestinationURL = System.getProperty( KETTLE_OPEN_LINEAGE_DESTINATION_URL.getConfigPropName(), openLineageDestinationURL );
  }

  private static List<OpenLineageSenderMode> getLineageModes() {
    List<OpenLineageSenderMode> result = new ArrayList<>();
    String config = System.getProperty( KETTLE_OPEN_LINEAGE_MODE.getConfigPropName(), "console" );
    String [] splitted = config.split( ";" );
    for (String mode : splitted) {
      result.add( OpenLineageSenderMode.valueOf( mode.toUpperCase() ) );
    }
    if (result.isEmpty()) {
      result.add( OpenLineageSenderMode.CONSOLE );
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
