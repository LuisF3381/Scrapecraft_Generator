import sys
from pathlib import Path

# Agrega la raiz del proyecto al sys.path para que pytest pueda importar
# los modulos 'src' y 'config' sin importar desde donde se invoque.
sys.path.insert(0, str(Path(__file__).parent))
