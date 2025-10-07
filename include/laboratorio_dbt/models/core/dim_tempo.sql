WITH instantes_unicos AS (
    SELECT
        DISTINCT instante
    FROM {{ ref('stg_usina_disp') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['instante::STRING']) }} AS id_dim_tempo,
    instante,
    EXTRACT(YEAR FROM instante)    AS ano,
    EXTRACT(MONTH FROM instante)   AS mes,
    EXTRACT(DAY FROM instante)     AS dia,
    EXTRACT(HOUR FROM instante)    AS hora,
    EXTRACT(DAYOFWEEK FROM instante) AS dia_da_semana -- Ex: 0=Domingo, 1=Segunda, etc. (varia com o DB)
FROM instantes_unicos
