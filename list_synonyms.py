import sempy.fabric as fabric
import sempy_labs
import sempy_labs.tom
from sempy_labs.tom import connect_semantic_model
import pandas as pd
import json
from typing import Optional
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

def list_synonyms(dataset: str, workspace: Optional[str] = None):

    workspace = fabric.resolve_workspace_name(workspace)

    df = pd.DataFrame(columns=['Culture Name', 'Table Name', 'Object Name', 'Object Type', 'Synonym', 'Type', 'State', 'Weight', 'Last Modified'])

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

                merged_terms = defaultdict(dict)
                for t in v.get('Terms', []):
                    for term, properties in t.items():
                        normalized_term = term.lower()
                        merged_terms[normalized_term].update(properties)
                 
                for term, props in merged_terms.items():
                    new_data = {
                        'Culture Name': lm.get('Language'),
                        'Table Name': table_name,
                        'Object Name': object_name,
                        'Object Type': object_type,                        
                        'Synonym': term,
                        'Type': props.get('Type'),
                        'State': props.get('State'),
                        'Weight': props.get('Weight'),
                        'Last Modified': props.get('LastModified')
                    }
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        
    df['Weight'] = df['Weight'].fillna(0).apply(Decimal)
    df['Weight'] = df['Weight'].apply(lambda x: x.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    df['Last Modified'] = pd.to_datetime(df['Last Modified'])

    return df
