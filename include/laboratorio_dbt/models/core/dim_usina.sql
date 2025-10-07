WITH usinas_unicas AS (
    SELECT
        DISTINCT
        ceg,
        nom_usina,
        nom_tipocombustivel
    FROM {{ ref('stg_usina_disp') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ceg', 'nom_usina', 'nom_tipocombustivel']) }} AS id_dim_usina,
    ceg,
    nom_usina,
    nom_tipocombustivel
FROM usinas_unicas
