import os
import pathlib
from typing import Any
from typing import Dict

import pandas as pd
from pandas.io.formats import excel

from . import BasePlugin
from . import pd_utils
from ..utils import SourceConfig
from ..utils import TargetConfig

from dbt.logger import GLOBAL_LOGGER as logger

class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config

        if "output" in plugin_config:
            assert isinstance(plugin_config["output"], dict)
            assert "file" in plugin_config["output"]

        # Pass s3 settings to plugin environment
        if "s3_access_key_id" in plugin_config:
            os.environ["AWS_ACCESS_KEY_ID"] = plugin_config["s3_access_key_id"]
        if "s3_secret_access_key" in plugin_config:
            os.environ["AWS_SECRET_ACCESS_KEY"] = plugin_config["s3_secret_access_key"]
        if "s3_region" in plugin_config:
            os.environ["AWS_DEFAULT_REGION"] = plugin_config["s3_region"]


    def load(self, source_config: SourceConfig):
        ext_location = source_config["external_location"]
        ext_location = ext_location.format(**source_config.as_dict())
        if "s3" in ext_location:
            # Possible to add some treatment in the future
            source_location = ext_location
        else:
            source_location = pathlib.Path(ext_location.strip("'"))
        sheet_name = source_config.get("sheet_name", 0)
        return pd.read_excel(source_location, sheet_name=sheet_name)


    def store(self, target_config: TargetConfig):
        plugin_output_config = self._config["output"]

        # Create the writer on the first instance of the call to store.
        # Instead if we instantiated the writer in the constructor
        # with mode = 'w', this would result in an existing file getting
        # overwritten. This can happen if dbt test is executed for example.
        if not hasattr(self, '_excel_writer'):
            self._excel_writer = pd.ExcelWriter(
                plugin_output_config["file"],
                mode=plugin_output_config.get("mode", "w"),
                engine=plugin_output_config.get("engine", "xlsxwriter"),
                engine_kwargs=plugin_output_config.get("engine_kwargs", {}),
                date_format=plugin_output_config.get("date_format"),
                datetime_format=plugin_output_config.get("datetime_format")
            )
            if "header_styling" in plugin_output_config and plugin_output_config["header_styling"] == False:
                excel.ExcelFormatter.header_style = None

        target_output_config = {**plugin_output_config, **target_config.config.get("overrides", {})}
        if not "sheet_name" in target_output_config:
            target_output_config["sheet_name"] = target_config.relation.identifier

        df = pd_utils.target_to_df(target_config)
        df.to_excel(
            self._excel_writer,
            sheet_name=target_output_config["sheet_name"],
            na_rep=target_output_config.get("na_rep", ""),
            float_format=target_output_config.get("float_format", None),
            header=target_output_config.get("header", True),
            index=target_output_config.get("index", True),
            merge_cells=target_output_config.get("merge_cells", True),
            inf_rep=target_output_config.get("inf_rep", "inf")
        )


    def __del__(self):
        if hasattr(self, '_excel_writer'):
            logger.info(f"Closing {self._config['output']['file']}")
            self._excel_writer.close()
