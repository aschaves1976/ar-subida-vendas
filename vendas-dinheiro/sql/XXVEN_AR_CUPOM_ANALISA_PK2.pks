--------------------------------------------------------
--  Arquivo criado - Sexta-feira-Julho-24-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package XXVEN_AR_CUPOM_ANALISA_PK2
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "APPS"."XXVEN_AR_CUPOM_ANALISA_PK2" as

procedure processar_dinheiro(errbuf out varchar2
,retcode out number
,p_data_ini in varchar2
,p_data_fim in varchar2);

procedure rotina_vendas_equals(errbuf out varchar2
,retcode out number
,p_lote_unico in varchar2);

procedure AtualizaBarramentoDinheiro(p_organizacao_venda in varchar2
, p_data_hora in date
, p_customer_trx_id in number
, p_erro in varchar2);

procedure rotina_recebimentos_equals(errbuf out varchar2
,retcode out number);

procedure processar_cupom(errbuf out varchar2
,retcode out number
,p_cupom in varchar2
,p_loja in varchar2
,p_pdv in varchar2
,p_data_ini in varchar2
,p_data_fim in varchar2);

procedure processar_mov_equals(errbuf out varchar2
,retcode out number
,p_lote_unico in varchar2);

procedure importar_movimentacao_equals;

procedure importar_arquivo_mov_venda(errbuf out varchar2
,retcode out number);

procedure importar_recebimento_equals(errbuf out varchar2
,retcode out number);

procedure criar_recebimento_equals(errbuf out varchar2
,retcode out number);

procedure criar_recebimento(p_amount in number
, p_receipt_number in varchar2
, p_receipt_date in date
, p_customer_id in number
, p_receipt_method_id in number
, p_org_id in number
, p_comments in varchar2
, p_msg_error out varchar2
, p_cr_id out number);

procedure processar_movimentacao_estoque(p_id_sequencial IN NUMBER); 

function valida_cliente(p_sequencial in number
, p_organizacao_venda in varchar2
, p_operating_unit in org_organization_definitions.operating_unit%type
, p_ship_to out hz_cust_site_uses_all.site_use_id%type
, p_bill_to out hz_cust_site_uses_all.site_use_id%type
, p_account_number out hz_cust_accounts.account_number%type
, p_cust_account_id out hz_cust_accounts.cust_account_id%type
, tipo in varchar2 default 'N') return boolean; 

function valida_cliente_cartao(p_sequencial in number
, p_organizacao_venda in varchar2
, p_operating_unit in org_organization_definitions.operating_unit%type
, p_ship_to out hz_cust_site_uses_all.site_use_id%type
, p_bill_to out hz_cust_site_uses_all.site_use_id%type
, p_account_number out hz_cust_accounts.account_number%type
, p_cust_account_id out hz_cust_accounts.cust_account_id%type
, p_adiquirente in varchar2 ) return boolean; 


function valida_itens(p_sequencial in number 
, p_organization_id in number
, p_operating_unit in number) return boolean; 

function get_batch_source(p_sequencial in number
, p_organizacao_venda in varchar2
, p_operating_unit in number
, p_batch_source_name out varchar2) return boolean;

function get_tipo_transacao(p_operating_unit in number
, p_sequencial in number
, p_cust_trx_type_id out number)return boolean;

function processa_pagamento_pedido(p_sequencial in number
, p_operating_unit in number
, p_organization_id in number
, p_organizacao_venda in varchar2
, p_cupom in varchar2
, p_caixa in varchar2) return boolean;

function processar_cupom_ar(p_sequencial in number
, p_pbm_autorizacao in varchar2
, p_organizacao_venda in varchar2
, p_operating_unit in org_organization_definitions.operating_unit%type 
, p_organization_id in org_organization_definitions.organization_id%type
, p_set_of_books_id in org_organization_definitions.set_of_books_id%type
, p_pbm_empresa_cliente in ar_customers.customer_name%type
, p_cust_trx_type_id in ra_cust_trx_types_all.cust_trx_type_id%type
, p_ship_to in hz_cust_site_uses_all.cust_acct_site_id%type
, p_bill_to in hz_cust_site_uses_all.cust_acct_site_id%type
, p_account_number in hz_cust_accounts.account_number%type
, p_cust_account_id in hz_cust_accounts.cust_account_id%type
, p_batch_source_name in ra_batch_sources_all.name%type
, p_term_id in ra_terms.term_id%type
, p_autoricazao in varchar2
) return boolean;

procedure movimentacao_estoque (p_item in mtl_system_items_b.inventory_item_id%type
, p_organization_id in org_organization_definitions.organization_id%type
, p_uom in mtl_system_items_b.primary_uom_code%type 
, p_transaction_date in mtl_transactions_interface.transaction_date%type
, p_transaction_reference in mtl_transactions_interface.transaction_reference%type
, p_cost_sales in mtl_parameters.cost_of_sales_account%type
, p_distribution in mtl_parameters.cost_of_sales_account%type
, p_transaction_quantity in mtl_transactions_interface.transaction_quantity%type
, p_id_ped_venda_cab in number
, p_id_ped_venda_lin in number
, p_transaction_cost in number
) ;

function get_balance_item(p_inventory_item_id in mtl_system_items_b.inventory_item_id%type
, p_organization_id in org_organization_definitions.organization_id%type) return number;

procedure movimentacao_estoque_interface(p_inventory_item_id in mtl_transactions_interface.inventory_item_id%type
, p_organization_id in mtl_transactions_interface.organization_id%type
, p_quantity_remaining in mtl_transactions_interface.transaction_quantity%type
, p_uom_code in mtl_transactions_interface.transaction_uom%type 
, p_distribution_id in mtl_transactions_interface.distribution_account_id%type 
, p_misc in mtl_transactions_interface.distribution_account_id%type 
, p_cupom_venda in mtl_transactions_interface.transaction_reference%type
, p_transaction_date in mtl_transactions_interface.transaction_date%type
, p_diferenca in mtl_transactions_interface.transaction_quantity%type
, p_transaction_cost in mtl_transactions_interface.transaction_cost%type); 

function file_exist(p_file in varchar2)return boolean; 

function file_exist_rec(p_file in varchar2) return boolean;

FUNCTION get_terms(
p_lote_unico IN VARCHAR2
, p_qtd IN NUMBER
, p_terms_id OUT NUMBER
, p_adiquirente IN NUMBER -- ASChaves 20190130 - Identificar o cliente
, p_org_id IN NUMBER
)
RETURN BOOLEAN
;
procedure Set_Atualiza_Parcela(p_lote_unico in varchar2
, p_header_id in number
, p_tipo_movimento in varchar2
, p_customer_trx_id in number); 

function GetContaBancaria(p_banco in varchar2
, p_conta in varchar2
, p_customer_id in number
, p_receipt_method_id out number) return boolean;

procedure atualiza_tabela_auxiliar(p_receipt_number in varchar2
, p_header_id in number
, p_cr_id in number
, p_msg in varchar2
, p_tipo in number);

procedure aplicar_recebimento(p_cr_id in number
, p_receipt_number in varchar2);

function ValidacaoRegistroExistente(p_estabelecimento in varchar2
, p_adiquirente in varchar2
, p_data_movimento in date
, p_lote_unico in varchar2
, p_parcela in number
, p_valor_bruto in number ) return boolean; 

function GetAtividade(p_estabelecimento in varchar2
, p_tipo_movimento in varchar2
, p_adiquirente in varchar2
, p_activity out varchar2
, p_erro out varchar2) return boolean; 

procedure ApplyReceipts(errbuf out varchar2
,retcode out number
,p_dataini in varchar2
,p_datafim in varchar2); 

procedure summary_sale_pdv(errbuf out varchar2
,retcode out number
,p_dataini in varchar2
,p_datafim in varchar2 );

function  GetAppliedBalance(p_lote_unico in varchar2
                          , p_parcela    in varchar2
                          , p_cash_receipt_id in number) return number;

procedure CarregarRecebimentosAgrupados(p_header_id IN NUMBER);
end xxven_ar_cupom_analisa_pk2;

/
