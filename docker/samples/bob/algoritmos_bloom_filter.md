# Filtros de Bloom en BlueRed

## Motivacion

Para evitar inundar la red con consultas federadas innecesarias (`Cas_11`),
cada par construye un Filtro de Bloom sobre los tokens normalizados de sus
archivos compartidos mediante `BloomFilterRepositoryDecorator`.

## Propiedades

- Cero falsos negativos: si un par posee el token, su filtro garantiza
  coincidencia positiva.
- Rechazo O(1) en busquedas locales negativas sin consultar SQLite.
- Serializacion compacta en bytes crudos almacenada en Redis junto a la
  sesion activa del par.
