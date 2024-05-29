package com.pentaho.di.trans.producer;

import io.openlineage.client.OpenLineage;

public interface IOpenLineageProducer {
  void emit( OpenLineage.RunEvent event );

  void emit( OpenLineage.JobEvent event );

  void emit( OpenLineage.DatasetEvent event );
}
