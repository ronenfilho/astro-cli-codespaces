WITH localidades_unicas AS (
    SELECT
        DISTINCT
        nom_subsistema,
        nom_estado
    FROM {{ ref('stg_usina_disp') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['nom_subsistema', 'nom_estado']) }} AS id_dim_localidade,
    nom_subsistema,
    nom_estado
FROM localidades_unicas
