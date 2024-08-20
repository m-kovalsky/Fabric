import sempy.fabric as fabric
import sempy_labs
from sempy_labs.tom import connect_semantic_model
import pandas as pd
import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

def list_synonyms(dataset: str, workspace: Optional[str] = None):

    workspace = fabric.resolve_workspace_name(workspace)

    df = pd.DataFrame(columns=['Culture Name', 'Table Name', 'Object Name', 'Object Type', 'State', 'Synonym', 'Type', 'Weight'])

    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        for c in tom.model.Cultures:
            lm = json.loads(c.LinguisticMetadata.Content)
            for k, v in lm.get('Entities', []).items():
                binding = v.get('Definition', {}).get('Binding', {})
                
                table_name = binding.get('ConceptualEntity')
                object_name = binding.get('ConceptualProperty')

                if object_name is None:
                    object_type = 'Table'
                    object_name = table_name
                elif any(m.Name == object_name and m.Parent.Name == table_name for m in tom.all_measures()):
                    object_type = 'Measure'
                elif any(m.Name == object_name and m.Parent.Name == table_name for m in tom.all_columns()):
                    object_type = 'Column'
                elif any(m.Name == object_name and m.Parent.Name == table_name for m in tom.all_hierarchies()):
                    object_type = 'Hierarchy'

                for t in v.get('Terms'):
                    syn = t.keys()
                    for k2, v2 in t.items():
                        syn_type = v2.get('Type')
                        syn_weight = v2.get('Weight')

                        new_data = {
                        'Culture Name': lm.get('Language'),
                        'Table Name': table_name,
                        'Object Name': object_name,
                        'Object Type': object_type,
                        'State': v.get('State'),
                        'Synonym': syn,
                        'Type': syn_type,
                        'Weight': syn_weight
                
                        }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        
        df['Weight'] = df['Weight'].fillna(0).apply(Decimal)
        df['Weight'] = df['Weight'].apply(lambda x: x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

    return df
         
