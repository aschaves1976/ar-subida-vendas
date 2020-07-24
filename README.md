**

## AR Subida de Vendas

### Folders

 - /vendas-conciliadas
 - /vendas-convenio
 - /vendas-dinheiro
---
### Concurrent

#### Programs:

  |Program Name| Short Name| Application| Description  | Executable |
  |--|--|--|--|--|
  | XXVEN - Subida de Vendas Conciliadas | XXVEN_SUB_MOV_EQUALS | Custom Application Venancio | XXVEN - Subida de Vendas Conciliadas | XXVEN_SUB_MOV_EQUALS |
  | XXVEN - Subida de Vendas em Dinheiro | XXVEN_SUBIDA_VENDAS_DINHEIRO | Custom Application Venancio | XXVEN - Subida de Vendas em Dinheiro | XXVEN_AR_CUPOM_ANALISA_PK2 |
  | XXVEN - Subida de vendas convênio | XXVEN_VENDA_CONVENIO | Custom Application Venancio | XXVEN - Subida de vendas convênio | XXVEN_AR_NEW_CONVENIO |

#### Executables:

  | Concurrent | Executable | Short Name | Application | Description | Exec File Name |
  |--|--|--|--|--|--|
  | XXVEN - Subida de Vendas Conciliadas | XXVEN_SUB_MOV_EQUALS | XXVEN_SUB_MOV_EQUALS | Custom Application Venancio | XXVEN_SUB_MOV_EQUALS | xxven_ar_cupom_analisa_pk2.rotina_vendas_equals |
  | XXVEN - Subida de Vendas em Dinheiro | XXVEN_AR_CUPOM_ANALISA_PK2 | XXVEN_AR_CUPOM_ANALISA_PK2 | Custom Application Venancio | XXVEN_AR_CUPOM_ANALISA_PK2 | xxven_ar_cupom_analisa_pk2.processar_dinheiro |
  | XXVEN - Subida de vendas convênio | XXVEN_AR_NEW_CONVENIO | XXVEN_AR_NEW_CONVENIO | Custom Application Venancio | XXVEN_AR_NEW_CONVENIO | xxven_ar_cupom_analisa_pk2.processar_cupom |

---
### Responsibility

  | Concurrent |Responsibility|  Request Group| Application |
  |--|--|--|--|
  | XXVEN - Subida de Vendas Conciliadas | DV AR SUPER USUARIO | JLBR + AR Reports | Latin America Localizations |
  | XXVEN - Subida de Vendas em Dinheiro | DV AR SUPER USUARIO | JLBR + AR Reports | Latin America Localizations |
  | XXVEN - Subida de vendas convênio | DV AR SUPER USUARIO | JLBR + AR Reports | Latin America Localizations |

---
### File

 - XXVEN_AR_CUPOM_ANALISA_PK2.pks
 - XXVEN_AR_CUPOM_ANALISA_PK2.pkb