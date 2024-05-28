package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;

public interface IOpenLineageSender  {
  public void emit( OpenLineage.RunEvent event );
  public void emit( OpenLineage.JobEvent event );
  public void emit( OpenLineage.DatasetEvent event );
}
