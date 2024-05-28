package org.pentaho.di.trans;

import io.openlineage.client.OpenLineage;

import java.net.InterfaceAddress;

public interface IOpenLineageWriter {

  void emit( OpenLineage.RunEvent event );
}
