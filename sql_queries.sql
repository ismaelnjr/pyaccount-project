-- SQL parametrizadas (usadas via pyodbc com placeholders "?")

-- Plano de Contas
-- params: (CODI_EMP)
SELECT 
  CODI_EMP,
  CODI_CTA,
  NOME_CTA,
  CLAS_CTA,
  TIPO_CTA,
  DATA_CTA,
  SITUACAO_CTA
FROM 
  BETHADBA.CTCONTAS
WHERE 
  CODI_EMP = ?
ORDER BY CLAS_CTA;

-- Lançamentos do período
-- params: (CODI_EMP, DATA_INI, DATA_FIM)
SELECT  
 l.codi_emp,
 l.nume_lan, 
 l.data_lan, 
 l.vlor_lan,
 l.cdeb_lan,
 l.ccre_lan,
 l.codi_his,
 l.chis_lan,
 l.ndoc_lan,
 l.codi_lote,
 t.tipo,
 l.codi_usu
FROM 
 BETHADBA.CTLANCTO l
 JOIN BETHADBA.CTLANCTOLOTE t
   ON l.codi_emp = t.codi_emp
  AND l.codi_lote = t.codi_lote
WHERE 
 l.codi_emp = ?
 AND l.data_lan BETWEEN ? AND ?
ORDER BY l.data_lan, l.nume_lan;

-- Saldos históricos até D-1 (agregando lançamentos)
-- params: (CODI_EMP, DATA_CORTE, CODI_EMP, DATA_CORTE)
SELECT conta, SUM(valor) AS saldo
FROM (
    SELECT l.cdeb_lan AS conta, SUM(l.vlor_lan) AS valor
      FROM BETHADBA.CTLANCTO l
     WHERE l.codi_emp = ?
       AND l.data_lan <= ?
     GROUP BY l.cdeb_lan
    UNION ALL
    SELECT l.ccre_lan AS conta, -SUM(l.vlor_lan) AS valor
      FROM BETHADBA.CTLANCTO l
     WHERE l.codi_emp = ?
       AND l.data_lan <= ?
     GROUP BY l.ccre_lan
) X
GROUP BY conta
HAVING SUM(valor) <> 0
ORDER BY conta;
