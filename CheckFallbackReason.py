import sempy
import sempy.fabric as fabric
import numpy as np
import pandas as pd

datasetName = '' #Enter dataset name
x = fabric.evaluate_dax(
        datasetName,
        """
        SELECT [TableName] AS [Table Name],[FallbackReason] AS [FallbackReasonID]
        FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES
        """)
x

value_mapping = {
    0: 'No reason for fallback',
    1: 'This table is not framed',
    2: 'This object is a view in the lakehouse',
    3: 'The table does not exist in the lakehouse',
    4: 'Transient error',
    5: 'Using OLS will result in fallback to DQ',
    6: 'Using RLS will result in fallback to DQ'
}

# Create a new column based on the mapping
x['Fallback Reason Detail'] = np.vectorize(value_mapping.get)(x['FallbackReasonID'])
x
