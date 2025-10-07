-- models/stg_usina_disp.sql

-- Bloco de código para os dados de Julho
SELECT
    '2025-07-01'::DATE AS mes_referencia,
    id_subsistema::STRING AS id_subsistema,
    nom_subsistema::STRING AS nom_subsistema,
    id_estado::STRING AS id_estado,
    nom_estado::STRING AS nom_estado,
    id_ons::STRING AS id_ons,
    ceg::STRING AS ceg,
    nom_usina::STRING AS nom_usina,
    id_tipousina::STRING AS id_tipousina,
    nom_tipocombustivel::STRING AS nom_tipocombustivel,
    din_instante::TIMESTAMP AS instante,
    val_potenciainstalada::NUMBER(38, 5) AS pot_instalada_mw,
    val_dispoperacional::NUMBER(38, 5) AS disp_operacional_mw,
    val_dispsincronizada::NUMBER(38, 5) AS disp_sincronizada_mw
FROM {{ source('staging','disponibilidade_usina_2025_07') }}

UNION ALL

-- Bloco de código para os dados de Agosto
SELECT
    '2025-08-01'::DATE AS mes_referencia,
    id_subsistema::STRING AS id_subsistema,
    nom_subsistema::STRING AS nom_subsistema,
    id_estado::STRING AS id_estado,
    nom_estado::STRING AS nom_estado,
    id_ons::STRING AS id_ons,
    ceg::STRING AS ceg,
    nom_usina::STRING AS nom_usina,
    id_tipousina::STRING AS id_tipousina,
    nom_tipocombustivel::STRING AS nom_tipocombustivel,
    din_instante::TIMESTAMP AS instante,
    val_potenciainstalada::NUMBER(38, 5) AS pot_instalada_mw,
    val_dispoperacional::NUMBER(38, 5) AS disp_operacional_mw,
    val_dispsincronizada::NUMBER(38, 5) AS disp_sincronizada_mw
FROM {{ source('staging','disponibilidade_usina_2025_08') }}

UNION ALL

-- Bloco de código para os dados de Setembro
SELECT
    '2025-09-01'::DATE AS mes_referencia,
    id_subsistema::STRING AS id_subsistema,
    nom_subsistema::STRING AS nom_subsistema,
    id_estado::STRING AS id_estado,
    nom_estado::STRING AS nom_estado,
    id_ons::STRING AS id_ons,
    ceg::STRING AS ceg,
    nom_usina::STRING AS nom_usina,
    id_tipousina::STRING AS id_tipousina,
    nom_tipocombustivel::STRING AS nom_tipocombustivel,
    din_instante::TIMESTAMP AS instante,
    val_potenciainstalada::NUMBER(38, 5) AS pot_instalada_mw,
    val_dispoperacional::NUMBER(38, 5) AS disp_operacional_mw,
    val_dispsincronizada::NUMBER(38, 5) AS disp_sincronizada_mw
FROM {{ source('staging','disponibilidade_usina_2025_09') }}
