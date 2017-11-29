package io.druid.metadata.storage.clickhouse;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.guice.SQLMetadataStorageDruidModule;
import io.druid.initialization.DruidModule;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageProvider;
import io.druid.metadata.NoopMetadataStorageProvider;
import io.druid.metadata.SQLMetadataConnector;

import java.util.List;

public class ClickhouseMetadataStorageModule extends SQLMetadataStorageDruidModule implements DruidModule
{
  public static final String TYPE = "clickhouse";

  public ClickhouseMetadataStorageModule()
  {
    super(TYPE);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageProvider.class))
        .addBinding(TYPE)
        .to(NoopMetadataStorageProvider.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageConnector.class))
        .addBinding(TYPE)
        .to(ClickhouseConnector.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(SQLMetadataConnector.class))
        .addBinding(TYPE)
        .to(ClickhouseConnector.class)
        .in(LazySingleton.class);
  }
}
