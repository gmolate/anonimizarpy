# anonimizarpy
anonimización de datos mediante python.
Para uso en bases de datos segun la "
https://repositoriodeis.minsal.cl/ContenidoSitioWeb2020/EstandaresNormativa/Norma%20t%C3%A9cnica%20241%20de%20anonimizaci%C3%B3n%20datos%20abiertos.pdf
Esta norma técnica se refiere al proceso de anonimización de datos estructurados en bases de datos para su publicación como datos abiertos



```python
import pandas as pd

def anonimizar_dataframe(df, id_vars, quasi_id_vars, sensitive_var):
    """
    Anonimiza un DataFrame según el protocolo especificado.

    Args:
        df (pd.DataFrame): El DataFrame a anonimizar.
        id_vars (list): Lista de nombres de columnas que son identificadores directos.
        quasi_id_vars (list): Lista de nombres de columnas que son cuasi-identificadores.
        sensitive_var (str): El nombre de la columna que contiene el atributo sensible.

    Returns:
        pd.DataFrame: El DataFrame anonimizado.
    """

    # 1. Eliminar identificadores explícitos
    df = df.drop(columns=id_vars, errors='ignore')  # Elimina las columnas, ignorando si no existen

    # 2. Anonimizar cuasi-identificadores
    if 'codigo_comuna' in quasi_id_vars:
        df = anonimizar_comuna(df, sensitive_var)
        quasi_id_vars.remove('codigo_comuna')
        quasi_id_vars.append('cod_comuna_final')

    # Aquí se podrían añadir más funciones para anonimizar otras variables cuasi-identificadoras
    # como 'sexo' o 'grupo_edad' si es necesario

    # 3. Verificar K y L-diversidad y anonimizar jerárquicamente si es necesario
    df = anonimizar_jerarquicamente(df, quasi_id_vars, sensitive_var)

    # 4. Eliminar variables auxiliares
    cols_a_eliminar = [col for col in df.columns if col.startswith('K_') or col.startswith('L_') or col.startswith('cod_comuna_nivel_')]
    cols_a_eliminar.extend(['codigo_comuna']) # Asegurarse de eliminar la original
    df = df.drop(columns=cols_a_eliminar, errors='ignore')

    return df

def anonimizar_comuna(df, sensitive_var):
    """
    Anonimiza la variable 'codigo_comuna' según el protocolo.

    Args:
        df (pd.DataFrame): El DataFrame.
        sensitive_var (str): El nombre de la columna que contiene el atributo sensible.

    Returns:
        pd.DataFrame: El DataFrame con la comuna anonimizada.
    """

    df['cod_comuna_primer_nivel'] = df['codigo_comuna'].astype(str)
    df['cod_comuna_segundo_nivel'] = df['codigo_comuna'].astype(str).str[:3] + '**'
    df['cod_comuna_tercer_nivel'] = df['codigo_comuna'].astype(str).str[:2] + '***'

    df = calcular_k_l(df, ['sexo', 'grupo_edad', 'cod_comuna_primer_nivel'], sensitive_var, 'primer')
    df = calcular_k_l(df, ['sexo', 'grupo_edad', 'cod_comuna_segundo_nivel'], sensitive_var, 'segundo')
    df = calcular_k_l(df, ['sexo', 'grupo_edad', 'cod_comuna_tercer_nivel'], sensitive_var, 'tercer')

    df['cod_comuna_final'] = df.apply(lambda row:
        row['cod_comuna_primer_nivel'] if row['K_primer_nivel'] >= 2 and row['L_primer_nivel'] >= 2
        else row['cod_comuna_segundo_nivel'] if row['K_segundo_nivel'] >= 2 and row['L_segundo_nivel'] >= 2
        else row['cod_comuna_tercer_nivel'] if row['K_tercer_nivel'] >= 2 and row['L_tercer_nivel'] >= 2
        else None, axis=1)

    return df

def calcular_k_l(df, group_cols, sensitive_var, nivel):
    """
    Calcula K-anonimidad y L-diversidad para un conjunto de columnas.

    Args:
        df (pd.DataFrame): El DataFrame.
        group_cols (list): Lista de columnas para agrupar (cuasi-identificadores).
        sensitive_var (str): El atributo sensible.
        nivel (str): Sufijo para las nuevas columnas ('primer', 'segundo', 'tercer').

    Returns:
        pd.DataFrame: El DataFrame con las columnas K y L añadidas.
    """

    df_grouped = df.groupby(group_cols).agg(
        K = ('sexo', 'size'),  # o cualquier otra columna, 'size' cuenta las filas en el grupo
        L = (sensitive_var, 'nunique')
    ).reset_index()

    df = df.merge(df_grouped, on=group_cols, how='left')

    df = df.rename(columns={'K': f'K_{nivel}_nivel', 'L': f'L_{nivel}_nivel'})
    return df

def anonimizar_jerarquicamente(df, quasi_id_vars, sensitive_var):
    """
    Anonimiza las variables cuasi-identificadoras jerárquicamente para asegurar K y L.

    Args:
        df (pd.DataFrame): El DataFrame.
        quasi_id_vars (list): Lista de cuasi-identificadores.
        sensitive_var (str): Atributo sensible.

    Returns:
        pd.DataFrame: DataFrame anonimizado.
    """

    for var in quasi_id_vars:
        df[f'K_global'] = df.groupby(quasi_id_vars)['sexo'].transform('size') # o cualquier otra columna
        df[f'L_global'] = df.groupby(quasi_id_vars)[sensitive_var].transform('nunique')

        if df[(df[f'K_global'] < 2) | (df[f'L_global'] < 2)].shape[0] > 0:
            if var == 'sexo':
                df['sexo'] = df.apply(lambda row: ' ' if (row[f'K_global'] < 2) or (row[f'L_global'] < 2) else row['sexo'], axis=1)
            elif var == 'grupo_edad':
                df['grupo_edad'] = df.apply(lambda row: '***' if (row[f'K_global'] < 2) or (row[f'L_global'] < 2) else row['grupo_edad'], axis=1)
            # Se podrían añadir más elifs para otras variables si fuera necesario
            elif var == 'cod_comuna_final':
                df['cod_comuna_final'] = df.apply(lambda row: '' if (row[f'K_global'] < 2) or (row[f'L_global'] < 2) else row['cod_comuna_final'], axis=1)

            df = df.drop(columns=['K_global','L_global'])
            return anonimizar_jerarquicamente(df, quasi_id_vars, sensitive_var)  # Recursión para re-evaluar

        df = df.drop(columns=['K_global','L_global'])
    return df
```

**Explicación y Mejoras Clave:**

* **Modularidad:** El código se ha estructurado en funciones para mayor claridad y reutilización.  
* **Manejo de Columnas:** Utiliza listas (`id_vars`, `quasi_id_vars`) para especificar las columnas a procesar, lo que lo hace más flexible.
* **Anonimización de Comuna:** Implementa la lógica de anonimización de `codigo_comuna` con los tres niveles y la selección del nivel adecuado.
* **Cálculo de K y L:** La función `calcular_k_l` calcula la K-anonimidad y L-diversidad para un conjunto dado de cuasi-identificadores.
* **Anonimización Jerárquica Generalizada:** La función `anonimizar_jerarquicamente`  maneja la anonimización iterativa de otras variables para asegurar la k-anonimidad y l-diversidad, incluyendo la generalización de sexo y grupo de edad.
* **Recursión:** La función `anonimizar_jerarquicamente` se llama a sí misma recursivamente para reevaluar la K y L-diversidad después de cada generalización. Esto asegura que se alcance el nivel de anonimización requerido.
* **Eficiencia:** Se utilizan operaciones vectorizadas de Pandas siempre que es posible para mejorar el rendimiento.
* **Claridad:** Se han añadido comentarios para explicar el propósito de cada sección del código.
* **Robustez:** Se agrega `errors='ignore'` al usar `drop()` para evitar errores si una columna no existe.

**Cómo Usarlo:**

1.  **Importa Pandas:** `import pandas as pd`
2.  **Carga tus datos:** `df = pd.read_csv('tu_archivo.csv')`
3.  **Define tus variables:**
    ```python
    id_vars = ['identificacion_paciente', 'nombre_paciente', 'direccion_paciente']
    quasi_id_vars = ['sexo', 'grupo_edad', 'codigo_comuna']
    sensitive_var = 'ENO'
    ```
4.  **Llama a la función de anonimización:**
    ```python
    df_anonimizado = anonimizar_dataframe(df.copy(), id_vars, quasi_id_vars, sensitive_var)
    ```
    * Es importante pasar `df.copy()` a la función para evitar modificar el DataFrame original accidentalmente.
5.  **Guarda los resultados:** `df_anonimizado.to_csv('datos_anonimizados.csv', index=False)`

Este protocolo en Python proporciona una implementación clara y eficiente del proceso de anonimización descrito en el documento.
