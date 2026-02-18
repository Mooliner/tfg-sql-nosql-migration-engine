from datetime import datetime

class DataTransformer:
    def __init__(self):
        self.db = "postgresql_tfg"


    def transformar(self, dada):
        nova_dada = dada.copy()
    
        if nova_dada.get("created_at") is not None:
            nova_dada["created_at"] = datetime.fromtimestamp(nova_dada["created_at"] / 1000000)
    
        if nova_dada.get("updated_at") is not None:
            nova_dada["updated_at"] = datetime.fromtimestamp(nova_dada["updated_at"] / 1000000)
    
        nova_dada["migrat_el"] = datetime.now()
        nova_dada["font"] = self.db

        return nova_dada