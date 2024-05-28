package org.pentaho.di.trans;

import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_ACTIVE;
import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_DESTINATION_URL;
import static org.pentaho.di.trans.OpenLineageConfigNames.KETTLE_OPEN_LINEAGE_MODE;

/**
 * A single point of access for all Open Lineage configuration properties.
 */
public class OpenLineageConfig {

  //Default values
  private Boolean openLineageActive = false;
  private String openLineageMode = "Console";
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
    openLineageMode = System.getProperty( KETTLE_OPEN_LINEAGE_MODE.getConfigPropName(), openLineageMode );
    openLineageDestinationURL = System.getProperty( KETTLE_OPEN_LINEAGE_DESTINATION_URL.getConfigPropName(), openLineageDestinationURL );
  }


  public Boolean getOpenLineageActive() {
    return openLineageActive;
  }

  public String getOpenLineageMode() {
    return openLineageMode;
  }

  public String getOpenLineageDestinationURL() {
    return openLineageDestinationURL;
  }

}
