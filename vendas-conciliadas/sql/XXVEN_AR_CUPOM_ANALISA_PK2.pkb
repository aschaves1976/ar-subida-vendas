CREATE OR REPLACE PACKAGE BODY XXVEN_AR_CUPOM_ANALISA_PK2 AS
  -- $Header: XXVEN_AR_CUPOM_ANALISA_PK2.pkb 120.3 2020/07/24 00:00:00 appldev noship $
  -- +=================================================================+
  -- |                Copyright (c) 2019 Venancio                      |
  -- |         Rio de Janeiro, Brasil, All rights reserved.            |
  -- +=================================================================+
  -- | FILENAME                                                        |
  -- |   XXVEN_AR_CUPOM_ANALISA_PK2.pkb                                |
  -- |                                                                 |
  -- | PURPOSE                                                         |
  -- |   Subir as Vendas Conciliadas                                   |
  -- |                                                                 |
  -- | DESCRIPTION                                                     |
  -- |                                                                 |
  -- | PARAMETERS                                                      |
  -- |                                                                 |
  -- | CREATED BY                                                      |
  -- |                                                                 |
  -- | ALTERED BY                                                      |
  -- |   Alessandro Chaves   (2019/01/30)                              |
  -- |     get_terms: Inclusao dos parametros de entrada P_ADIQUIRENTE |
  -- |                e P_ORG_ID;                                      |
  -- |     get_terms: Query para identificar o TERM_ID alterada para   |
  -- |                localizar o term_id filrando por org_id e por    |
  -- |                adiquirente;                                     |
  -- |                                                                 |
  -- |   Alessandro Chaves   (2020/07/24)                              |
  -- |     summary_sale_pdv: Desabilitada a Movimentacao de Inventario |
  -- |                                                                 |
  -- +=================================================================+
  --

  procedure summary_sale_pdv(errbuf    out varchar2
                             ,retcode   out number
                             ,p_dataini in varchar2
                             ,p_datafim in varchar2) as
    w_organization_id org_organization_definitions.organization_id%type;
    w_cmv             mtl_parameters.cost_of_sales_account%type;
  begin
    for r_mov in(select lin.organizacao_venda, msi.primary_uom_code --, ood.organization_id
                    , lin.data_hora, msi.inventory_item_id, lin.codigo_item, sum(lin.quantidade) total_quantidade
                 from tb_anali_ebs_ped_venda_cab@intprd cab
                    , tb_anali_ebs_ped_venda_lin@intprd lin 
                    , mtl_system_items_b                msi
                where lin.id_ped_venda_cab = cab.id_sequencial
                  and nvl(lin.inv,'N') = 'N'
                  and cab.data_hora between nvl( to_date(to_date(p_dataini ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                                        and nvl( to_date(to_date(p_datafim ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                  and msi.segment1 = lin.codigo_item
                  and msi.organization_id = 174
               group by lin.organizacao_venda , msi.primary_uom_code, lin.data_hora, msi.inventory_item_id, lin.codigo_item
               order by lin.data_hora, lin.organizacao_venda,msi.inventory_item_id) loop

    begin
      select organization_id into w_organization_id
        from org_organization_definitions 
       where organization_code = r_mov.organizacao_venda;

      select cost_of_sales_account into w_cmv
        from mtl_parameters
       where organization_id = w_organization_id; 

      insert into inv.mtl_transactions_interface (transaction_interface_id
                                                , source_header_id
                                                , source_line_id
                                                , source_code
                                                , process_flag
                                                , transaction_mode
                                                , lock_flag
                                                , last_update_date
                                                , last_updated_by
                                                , creation_date
                                                , created_by
                                                , inventory_item_id
                                                , organization_id
                                                , transaction_quantity
                                                , transaction_uom
                                                , transaction_date
                                                , subinventory_code
                                                , transaction_type_id
                                                , transaction_source_type_id
                                                , transaction_reference
                                                , distribution_account_id
                                                , validation_required)
                                          values( mtl_material_transactions_s.nextval  --transaction_interface_id
                                                ,1                           --source_header_id
                                                ,1                           --source_line_id
                                                ,'Venda PDV'                 --source_code
                                                , 1                          --process_flag
                                                , 3                          --transaction_mode
                                                , 2                          --lock_flag 1 ou 2
                                                , sysdate                    --last_update_date
                                                , fnd_global.user_id         --last_updated_by
                                                , sysdate                    --creation_date
                                                , fnd_global.user_id         --created_by
                                                , r_mov.inventory_item_id    --p_inventory_item_id        --inventory_item_id
                                                , w_organization_id --p_organization_id          --organization_id
                                                , (-1* r_mov.total_quantidade)  --transaction_quantity
                                                , r_mov.primary_uom_code     --transaction_uom
                                                , r_mov.data_hora            --transaction_date
                                                , 'COMERC.'                  --subinventory_code
                                                , (select transaction_type_id from mtl_transaction_types where transaction_type_name = 'Venda PDV')--101 108                       --transaction_type_id
                                                , 13                         --transaction_source_type_id
                                                , 'Venda PDV'                --transaction_reference         cupom
                                                , w_cmv                     --distribution_account_id       cost sales
                                                , 1                          --VALIDATION_REQUIRED
                                                 );   
      -- ASChaves 20200724 - Inicio: Não haverá mais movimentação de inventário
        -- atualiza barramento
        -- update tb_anali_ebs_ped_venda_lin@intprd set inv = 'Y'
        --  where data_hora        = r_mov.data_hora
        --    and organizacao_venda = r_mov.organizacao_venda
        --    and codigo_item      = r_mov.codigo_item;
      -- ASChaves 20200724 - Fim: Não haverá mais movimentação de inventário

      commit;   
    exception
      when others then
        null;
    end;

  end loop;

  commit;  
  end;

  procedure rotina_vendas_equals(errbuf       out varchar2
                                ,retcode      out number
                                ,p_lote_unico in varchar2) as
  begin
    xxven_ar_cupom_analisa_pk2.importar_arquivo_mov_venda(errbuf,retcode);

    xxven_ar_cupom_analisa_pk2.processar_mov_equals(errbuf,retcode,p_lote_unico);
  end rotina_vendas_equals;

  procedure rotina_recebimentos_equals(errbuf    out varchar2
                                      ,retcode   out number) as
  begin
    xxven_ar_cupom_analisa_pk2.importar_recebimento_equals(errbuf, retcode);

    xxven_ar_cupom_analisa_pk2.criar_recebimento_equals(errbuf, retcode);

  end rotina_recebimentos_equals;

  procedure ApplyReceipts(errbuf    out varchar2
                         ,retcode   out number
                         ,p_dataini in varchar2
                         ,p_datafim in varchar2) as
  begin
    -- Aplicar recebimentos criados anteriormete e não aplicados ou parcialmente aplicados

    for r_apply in (select distinct acr.cash_receipt_id, acr.receipt_number ,acr.receipt_date
                      from ar_cash_receipts_all acr
                         , xxven_ar_rec_lines   arl
                     where acr.status          = 'UNAPP'
                       and arl.cash_receipt_id = acr.cash_receipt_id
                       and acr.type = 'CASH'
                       and acr.comments = 'EQUALS'
                       and acr.receipt_date between nvl( to_date(to_date(p_dataini ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),acr.receipt_date)
                                                and nvl( to_date(to_date(p_datafim ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),acr.receipt_date)
                       --and acr.cash_receipt_id in(763364)
                    order by acr.receipt_date
                    ) loop


      aplicar_recebimento(r_apply.cash_receipt_id, r_apply.receipt_number);  

    end loop; --r_apply

  end;

  function  GetAppliedBalance(p_lote_unico   in varchar2
                            , p_parcela      in varchar2
                            , p_cash_receipt_id in number) return number as
    w_saldo_aplicado number;
  begin
    select nvl(sum(ara.amount_applied),0) -- saldo_applicado
      into w_saldo_aplicado
      from ar_cash_receipts_all           acr
         , ar_receivable_applications_all ara
         , ar_payment_schedules_all       aps
         , ra_customer_trx_all            rct
     where 1=1
       and acr.cash_receipt_id             = ara.cash_receipt_id
       and ara.applied_payment_schedule_id = aps.payment_schedule_id
       and aps.customer_trx_id             = rct.customer_trx_id
       and ara.display                     = 'Y'
       and acr.cash_receipt_id              = p_cash_receipt_id
       and aps.terms_sequence_number       = p_parcela
       and rct.attribute11                 = p_lote_unico;

    return w_saldo_aplicado;   
  exception
    when others then
      return 0;
  end;

  procedure aplicar_recebimento(p_cr_id          in number
                              , p_receipt_number in varchar2) as
    w_msg_count     number;
    w_msg_data      varchar2(4000);
    w_return_status varchar2(500);

    w_apply_amount number;
    w_balance     number;
    w_data        date;
  begin
    --fnd_file.put_line(fnd_file.log,'passou 2');
    for r_rec in(select rec.cash_receipt_id, rec.parcela, aps.amount_due_remaining ,rec.lote_unico, aps.payment_schedule_id, rct.customer_trx_id
                      , rec.data_movimento, rct.trx_number, sum(rec.valor_liquido) valor_liquido
                   from xxven_ar_rec_lines       rec
                      , ra_customer_trx_all      rct
                      , ar_payment_schedules_all aps
                  where rec.status                in('P','E')
                 --   and nvl(rec.aplicado, 'E')    != 'E'
                    and rct.attribute11           = rec.lote_unico
                    and aps.customer_trx_id       = rct.customer_trx_id
                    and aps.terms_sequence_number = rec.parcela
                    and aps.amount_due_remaining  > 0
                    and rec.cash_receipt_id       = p_cr_id
               group by rec.cash_receipt_id, rec.parcela,  aps.amount_due_remaining ,rec.lote_unico, aps.payment_schedule_id, rct.customer_trx_id
                      , rec.data_movimento, rct.trx_number            
               order by lote_unico     
                    ) loop
      begin
        w_balance := null;
        w_data := null;
        w_balance := r_rec.valor_liquido - GetAppliedBalance(r_rec.lote_unico, r_rec.parcela, r_rec.cash_receipt_id) ;

        if r_rec.data_movimento < to_date('01/08/19','dd/mm/yy') then
          w_data := to_date('01/08/19','dd/mm/yy');
        else
          w_data := r_rec.data_movimento;
        end if;

        if w_balance > 0 then

          if w_balance < r_rec.amount_due_remaining then
            w_apply_amount := w_balance;
          elsif w_balance > r_rec.amount_due_remaining  then
            w_apply_amount := r_rec.amount_due_remaining;
          elsif w_balance = r_rec.amount_due_remaining then
            w_apply_amount := r_rec.amount_due_remaining;
          end if;

          AR_RECEIPT_API_PUB.apply(p_api_version                 => 1.0
                                  ,p_init_msg_list               => FND_API.G_TRUE
                                  ,p_commit                      => FND_API.G_TRUE
                                  ,p_validation_level            => FND_API.G_VALID_LEVEL_FULL
                                  ,p_receipt_number              => p_receipt_number
                                  ,p_cash_receipt_id             => p_cr_id 
                                  ,p_customer_trx_id             => r_rec.customer_trx_id 
                                  ,p_applied_payment_schedule_id => r_rec.payment_schedule_id
                                  ,p_amount_applied              => w_apply_amount --r_rec.amount_due_remaining --valor_liquido
                                  ,p_apply_date                  => w_data --r_rec.data_movimento
                                  ,p_apply_gl_date               => w_data --r_rec.data_movimento 
                                  ,p_customer_reference          => ''
                                  ,x_return_status               => w_return_status
                                  ,x_msg_count                   => w_msg_count
                                  ,x_msg_data                    => w_msg_data);
        ---
          fnd_file.put_line(fnd_file.log,'w_return_status: ' || w_return_status);
        end if;

        IF nvl(w_return_status,'N') = 'S' THEN
          if r_rec.amount_due_remaining = w_apply_amount then
            update xxven_ar_rec_lines set aplicado = 'Y' where lote_unico = r_rec.lote_unico and parcela = r_rec.parcela and cash_receipt_id = r_rec.cash_receipt_id;  

          elsif r_rec.amount_due_remaining > w_apply_amount then
            update xxven_ar_rec_lines set aplicado = 'P' where lote_unico = r_rec.lote_unico and parcela = r_rec.parcela and cash_receipt_id = r_rec.cash_receipt_id;  
          end if;  

          commit;

          /*
          update ar_receivable_applications_all set  acctd_amount_applied_to = acctd_amount_applied_from
           where Applied_Payment_Schedule_Id = r_rec.payment_schedule_id
             and customer_trx_id             = r_rec.customer_trx_id
             and cash_receipt_id             = r_rec.cash_receipt_id
             and application_type            = 'CASH'
             and display                     = 'Y';

          commit;
          */ 
          fnd_file.put_line(fnd_file.log,'Transação Nº: ' || r_rec.trx_number  || ' aplicado no recebimento ' || p_receipt_number);

        else
          FOR l IN 1 .. NVL(w_msg_count, 0) LOOP                         
            FND_MSG_PUB.GET(p_msg_index => l, p_encoded => 'F', p_data => w_msg_data, p_msg_index_out => w_msg_count);                         

            fnd_file.put_line(fnd_file.log,'Transação Nº: ' || r_rec.trx_number  || ' não aplicado no recebimento ' || p_receipt_number);
            fnd_file.put_line(fnd_file.log,   'TITULO NAO ASSOCIADO AO RECEBIMENTO Erro: ' || w_msg_data);                      

          END LOOP;

          --update xxven_ar_rec_lines set aplicado = 'E' where line_id = r_rec.line_id;
          update xxven_ar_rec_lines set aplicado = 'E' where lote_unico = r_rec.lote_unico and parcela = r_rec.parcela and cash_receipt_id = r_rec.cash_receipt_id;  
          commit;
        end if;

      end;

    end loop;

    for r_rec in(select rec.line_id, rec.cash_receipt_id, rec.parcela, rec.valor_liquido, aps.amount_due_remaining ,rec.lote_unico, aps.payment_schedule_id, rct.customer_trx_id, rec.data_movimento, rct.trx_number
                   from xxven_ar_rec_lines       rec
                      , ra_customer_trx_all      rct
                      , ar_payment_schedules_all aps
                  where rec.status                in('P','E') 
                  --and nvl(rec.aplicado, 'E')    = 'E'
                    and rct.attribute11           = (rec.lote_unico || '_' || rec.parcela)
                    and aps.customer_trx_id       = rct.customer_trx_id
                    and aps.terms_sequence_number = 1 --rec.parcela
                    and rec.cash_receipt_id       = p_cr_id) loop

      if r_rec.valor_liquido < r_rec.amount_due_remaining then
        w_apply_amount := r_rec.valor_liquido;
      elsif r_rec.valor_liquido > r_rec.amount_due_remaining  then
        w_apply_amount := r_rec.amount_due_remaining;
      else
        w_apply_amount := r_rec.valor_liquido ;
      end if;

      AR_RECEIPT_API_PUB.apply(p_api_version                 => 1.0
                              ,p_init_msg_list               => FND_API.G_TRUE
                              ,p_commit                      => FND_API.G_TRUE
                              ,p_validation_level            => FND_API.G_VALID_LEVEL_FULL
                              ,p_receipt_number              => p_receipt_number
                              ,p_cash_receipt_id             => p_cr_id 
                              ,p_customer_trx_id             => r_rec.customer_trx_id 
                              ,p_applied_payment_schedule_id => r_rec.payment_schedule_id
                              ,p_amount_applied              => r_rec.amount_due_remaining --valor_liquido
                              ,p_apply_date                  => r_rec.data_movimento
                              ,p_apply_gl_date               => r_rec.data_movimento 
                              ,p_customer_reference          => ''
                              ,x_return_status               => w_return_status
                              ,x_msg_count                   => w_msg_count
                              ,x_msg_data                    => w_msg_data);
      ---
      IF nvl(w_return_status,'N') = 'S' THEN
        update xxven_ar_rec_lines set aplicado = 'Y' where line_id = r_rec.line_id;  

        update ar_receivable_applications_all set  acctd_amount_applied_to = acctd_amount_applied_from
         where Applied_Payment_Schedule_Id = r_rec.payment_schedule_id
           and customer_trx_id             = r_rec.customer_trx_id
           and cash_receipt_id             = r_rec.cash_receipt_id
           and application_type            = 'CASH'
           and display                     = 'Y';


        fnd_file.put_line(fnd_file.log,'Transação Nº: ' || r_rec.trx_number  || ' aplicado no recebimento ' || p_receipt_number);

      else
        FOR l IN 1 .. NVL(w_msg_count, 0) LOOP                         
          FND_MSG_PUB.GET(p_msg_index => l, p_encoded => 'F', p_data => w_msg_data, p_msg_index_out => w_msg_count);                         

          fnd_file.put_line(fnd_file.log,'Transação Nº: ' || r_rec.trx_number  || ' não aplicado no recebimento ' || p_receipt_number);
          fnd_file.put_line(fnd_file.log,   'TITULO NAO ASSOCIADO AO RECEBIMENTO Erro: ' || w_msg_data);                      

        END LOOP;

        update xxven_ar_rec_lines set aplicado = 'E' where line_id = r_rec.line_id;
      end if;

      --end;
    end loop;
  end;

  procedure atualiza_tabela_auxiliar(p_receipt_number in varchar2
                                   , p_header_id      in number
                                   , p_cr_id          in number
                                   , p_msg            in varchar2
                                   , p_tipo           in number) as
  begin
    if p_tipo = 1 then
      for r_receb in(select x.* from
                   (select  SUBSTR(( substr(OTH.adiquirente, 1, 1) || '_' || substr(OTH.CONTA, -8, 8) || '_' || TO_CHAR( OTH.data_Movimento, 'DDMMYY')),1,30)receipt_number
                         , line_id
                         ,(select cash_receipt_id
                                     from ar_cash_receipts_all
                                    where receipt_number = p_receipt_number
                                      and cash_receipt_id = p_cr_id) cash_receipt_id
                     from ( select lin.line_id, lin.data_Movimento,decode(upper(lin.adiquirente), 'AMEX', 'CIELO',upper(lin.adiquirente))  adiquirente, 
                                   lin.banco, 
                                   lpad(lin.conta,20,'0') conta
                              from xxven_ar_rec_headers hea
                                 , xxven_ar_rec_lines   lin
                                 , org_organization_definitions ood
                             where hea.header_id = lin.header_id
                               and hea.status = 'U'
                               and lin.status = 'U' 
                               and lin.tipo_movimento in('D1','D2','D3')
                               and ood.organization_id = lin.organization_id
                               and lin.credito_debito = 'C'

                           ) oth) x
                              where x.receipt_number = p_receipt_number
                                ) loop

        if nvl(r_receb.cash_receipt_id,0) > 0 then
          update xxven_ar_rec_lines set status = 'P' , desc_status = ('Recebimento criado: ' || r_receb.receipt_number), cash_receipt_id = r_receb.cash_receipt_id
           where line_id = r_receb.line_id;
        else   
          null; --select * from xxven_ar_rec_lines
          update xxven_ar_rec_lines set status = 'E' , desc_status = p_msg 
           where line_id = r_receb.line_id;
        end if;
      end loop;
    elsif p_tipo = 2 then
      for r_receb in( select receipt_number
                           , x.line_id
                         from (select --SUBSTR(( substr(upper(acl.adiquirente), 1, 1) || '_' || substr(acl.tipo_movimento,1,1) || '_' || to_char(acl.data_movimento,'ddmmyy') || '_' || substr(acl.estabelecimento,-12,12) || '_' || acl.banco),1,30)RECEIPT_NUMBER 
                                      SUBSTR(( substr(upper(acl.adiquirente), 1, 1)  || '_' || substr(acl.tipo_movimento,1,1) || '_' || to_char(acl.data_movimento,'ddmmyy') || '_' || substr(lpad(acl.estabelecimento,12,'0'),-12,12) || '_' || acl.banco),1,30) RECEIPT_NUMBER
                                    , acl.line_id
                                 from xxven_ar_rec_lines          acl 
                                    , org_organization_definitions ood
                                where acl.tipo_movimento in('E1','E2','E3'
                                                           ,'F1','F2','F3'
                                                           ,'G1') 
                                  and acl.status = 'U'      
                                  and acl.organization_id = ood.organization_id 
                               union all
                               select SUBSTR(( substr(upper(acl.adiquirente), 1, 1)  || '_' || substr('E',1,1) || '_' || to_char(acl.data_movimento,'ddmmyy') || '_' || substr(lpad(acl.estabelecimento,12,'0'),-12,12) || '_' || acl.banco),1,30) RECEIPT_NUMBER
                                     , acl.line_id
                                 from xxven_ar_rec_lines          acl 
                                   , org_organization_definitions ood
                                where acl.tipo_movimento in('H2') 
                                  and acl.status = 'U' 
                                  and acl.credito_debito = 'D'
                                  and acl.organization_id = ood.organization_id   
                                  ) x
                        where x.receipt_number = p_receipt_number) loop

        if nvl(p_cr_id,0) > 0 then 
          update xxven_ar_rec_lines set status = 'P' , desc_status = p_msg, cash_receipt_id = p_cr_id
           where line_id = r_receb.line_id;
        else
          update xxven_ar_rec_lines set status = 'E' , desc_status = p_msg 
           where line_id = r_receb.line_id;
        end if;

      end loop;                  
    elsif p_tipo = 3 then
      for r_receb in( select receipt_number
                           , x.line_id
                         from (select SUBSTR(( substr(acl.adiquirente, 1, 1) || 'H' || '_' || substr(acl.CONTA, -8, 8) || '_' || TO_CHAR( acl.data_Movimento, 'DDMMYY') || '_' || to_char(sysdate,'ddmm') ),1,30)RECEIPT_NUMBER

                                    , acl.line_id
                                 from xxven_ar_rec_lines          acl 
                                    , org_organization_definitions ood
                                where acl.tipo_movimento in('H2') 
                                  and acl.status = 'U' 
                                  and acl.credito_debito = 'C'
                                  and acl.organization_id = ood.organization_id ) x
                        where x.receipt_number = p_receipt_number) loop

        if nvl(p_cr_id,0) > 0 then 
          update xxven_ar_rec_lines set status = 'P' , desc_status = p_msg, cash_receipt_id = p_cr_id
           where line_id = r_receb.line_id;
        else
          update xxven_ar_rec_lines set status = 'E' , desc_status = p_msg 
           where line_id = r_receb.line_id;
        end if;

      end loop;

    end if;

    commit;
  end;

  procedure criar_recebimento(p_amount            in number
                            , p_receipt_number    in varchar2
                            , p_receipt_date      in date
                            , p_customer_id       in number
                            , p_receipt_method_id in number
                            , p_org_id            in number
                            , p_comments          in varchar2
                            , p_msg_error         out varchar2
                            , p_cr_id             out number)as

    l_return_status VARCHAR2(500);
    l_msg_count     number;
    l_msg_data      VARCHAR2(4000);
    l_msg_error     VARCHAR2(10000);
  begin
    begin
      AR_RECEIPT_API_PUB.create_cash(p_api_version        => 1
                                    ,p_init_msg_list     => FND_API.G_TRUE
                                    ,p_commit            => FND_API.G_FALSE
                                    ,p_validation_level  => FND_API.G_VALID_LEVEL_FULL
                                    ,x_return_status     => l_return_status
                                    ,x_msg_count         => l_msg_count
                                    ,x_msg_data          => l_msg_data
                                    ,p_currency_code     => 'BRL'
                                    ,p_amount            => p_amount
                                    ,p_receipt_number    => p_receipt_number
                                    ,p_receipt_date      => p_receipt_date
                                    ,p_gl_date           => p_receipt_date
                                    ,p_customer_id       => p_customer_id -- r_receipt.pay_from_customer -- p_customer_number
                                    ,p_receipt_method_id => p_receipt_method_id
                                    ,p_org_id            => p_org_id --FND_GLOBAL.ORG_ID
                                    ,p_comments          => p_comments
                                    ,p_cr_id             => p_cr_id);
      l_msg_error := null;
      FOR l IN 1 .. NVL(l_msg_count, 0) LOOP
        begin
          FND_MSG_PUB.GET(p_msg_index => l, p_encoded => 'F', p_data => l_msg_data, p_msg_index_out => l_msg_count);
           --dbms_output.put_line('l_msg_data ' || l_msg_data);
          l_msg_error := l_msg_error || ' ' || l_msg_data;
        end;
      end loop;

      IF p_cr_id IS NULL THEN
        dbms_output.put_line('RECEBIMENTO NAO CRIADO Status: ' || l_return_status); 
        dbms_output.put_line('RECEBIMENTO NAO CRIADO Erro: ' || l_msg_error); 
        fnd_file.put_line(fnd_file.log, 'RECEBIMENTO NAO CRIADO Status: ' || l_return_status);
        fnd_file.put_line(fnd_file.log, 'RECEBIMENTO NAO CRIADO Erro: ' || l_msg_error);


      END IF;

      if l_return_status = 'S' then
        dbms_output.put_line('RECEBIMENTO CRIADO: ' || p_receipt_number); 
      end if;

    end;
  end;

  function GetContaBancaria(p_banco             in varchar2
                          , p_conta             in varchar2
                          , p_customer_id       in number
                          , p_receipt_method_id out number) return boolean as
  begin
    begin
      --dbms_output.put_line('Conta: ' || p_conta);
      --dbms_output.put_line('Cliente: ' || p_customer_id);

      SELECT RECEIPT_METHOD_ID
              INTO p_receipt_method_id
              FROM AR_RECEIPT_METHODS
             WHERE NAME = (SELECT DESCRIPTION
                             FROM FND_LOOKUP_VALUES
                            WHERE LOOKUP_TYPE = 'XXVEN_METODO_RECEBIMENTO_CC'
                              AND ATTRIBUTE5 = (SELECT NAME
                                                  FROM HR_ALL_ORGANIZATION_UNITS
                                                 WHERE ORGANIZATION_ID = FND_GLOBAL.ORG_ID 
                                                   AND ROWNUM = 1) 
                              AND ATTRIBUTE6 = (select decode(customer_name,'SODEXO PASS DO BRASIL SERVICOS E COMERCIO S.A.', 'SODEXO PASS DO BR SA', customer_name) customer_name 
                                                  from ar_customers where customer_id = p_customer_id)
                              AND TAG = p_conta
                              AND LANGUAGE = 'PTB');

      dbms_output.put_line('Achou o metodo de recebimento ');
      return true;
    exception
      when others then
        return false;
    end;
  end;

  procedure criar_recebimento_equals(errbuf    out varchar2
                                    ,retcode   out number) as
    w_customer_id       number;
    w_cr_id             number;
    w_receipt_method_id number ;
    w_msg               varchar2(240);
    w_activity          varchar2(50); 
    w_erro              varchar2(1000) := null; 
    w_count             number;  
    l_return_status     varchar2(1);
    l_msg_count         number;
    l_msg_data          varchar2(240);
  begin

    fnd_file.put_line(fnd_file.log,' ');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'********** INICIO DO PROCESSAMENTO RECEBIMENTO***********');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,' ');
    --- recebimentos
    -- Retiramos o banco do group by por conta de erro na origem da equals 29/04/19
    for r_rec in(
                select  SUBSTR(( substr(OTH.adiquirente, 1, 1) || '_' || substr(OTH.CONTA, -8, 8) || '_' || TO_CHAR( OTH.data_Movimento, 'DDMMYY')),1,30)RECEIPT_NUMBER,
                        OTH.data_Movimento, 
                        OTH.adiquirente, 
                        --OTH.banco, 
                        OTH.conta,
                        oth.operating_unit,
                        oth.header_id,
                        Sum (OTH.valor_Liquido) Valor_Liquido
                from
                ( 
                select  lin.data_Movimento, 
                        decode( upper(lin.adiquirente), 'AMEX', 'CIELO',upper(lin.adiquirente))  adiquirente, 
                        lin.banco, 
                        lpad(lin.conta,20,'0') conta,
                        ood.operating_unit,
                        hea.header_id,
                        Lin.valor_Liquido
                  from xxven_ar_rec_headers hea
                     , xxven_ar_rec_lines   lin
                     , org_organization_definitions ood
                 where hea.header_id = lin.header_id
                   and hea.status = 'U'
                   and lin.status != 'P' 
                   and ood.organization_id = lin.organization_id
                   and lin.tipo_movimento in('D1','D2','D3')
                   and lin.credito_debito = 'C'
                   AND lin.data_Movimento >= TO_DATE('05/07/18','DD/MM/YY') 
                   ) OTH
                group by OTH.data_Movimento, OTH.adiquirente --, OTH.banco
                                                              , OTH.conta, oth.operating_unit,oth.header_id
                  ) loop

      -- cliente 
      w_customer_id := null;
      if r_rec.adiquirente = 'CIELO' then
        w_customer_id := 10109;
      elsif r_rec.adiquirente = 'AMEX' then
        w_customer_id := 11110;
      elsif r_rec.adiquirente = 'REDE' then
        w_customer_id := 11108;
      elsif r_rec.adiquirente = 'SODEXO' then
        w_customer_id := 92861;
      end if;

      -- metodo de recebimento
      w_receipt_method_id := null;
      if GetContaBancaria(NULL, r_rec.conta, w_customer_id, w_receipt_method_id) then

        --trader
        w_cr_id := null;
        w_msg   := null;
        criar_recebimento(r_rec.valor_liquido   -- p_amount            in number
                        , r_rec.receipt_number  -- p_receipt_number    in varchar2
                        , r_rec.data_movimento  -- p_receipt_date      in date
                        , w_customer_id         --p_customer_id       in number
                        , w_receipt_method_id   --in number
                        , r_rec.operating_unit  --p_org_id            in number
                        , 'EQUALS'              -- p_comments          in varchar2
                        , w_msg
                        , w_cr_id);            

        if nvl(w_cr_id,0) > 0 then
          dbms_output.put_line('Não criou o recebimento: ' || r_rec.receipt_number);

          fnd_file.put_line(fnd_file.log,'Não criou o recebimento: ' || r_rec.receipt_number);
          w_msg := substr( 'Receipt number: ' || r_rec.receipt_number || ' ' || w_msg,1,240);

          atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 1);


        else
          dbms_output.put_line('criou o recebimento: ' || w_cr_id);

          w_msg := ('Recebimento criado: ' || r_rec.receipt_number);
          atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 1);

          fnd_file.put_line(fnd_file.log, w_msg);

          --aplicar_recebimento(w_cr_id, r_rec.receipt_number); 
          --aplicar_recebimento(w_cr_id, r_rec.receipt_number);
        end if;  
      else

        w_msg := 'Não foi possível localizar o método de recebimento para a Conta ' || lpad(r_rec.conta,20,'0')  || ' Cliente ' || r_rec.adiquirente; 
        fnd_file.put_line(fnd_file.log,w_msg);
        atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 1);
      end if;
      commit;
      --rollback;
    end loop;

   --Recebimento H2 C
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
   for r_rec in(
                select  SUBSTR(( substr(OTH.adiquirente, 1, 1) || 'H' || '_' || substr(OTH.CONTA, -8, 8) || '_' || TO_CHAR( OTH.data_Movimento, 'DDMMYY') || '_' || to_char(sysdate,'ddmm') ),1,30)RECEIPT_NUMBER,
                        OTH.data_Movimento, 
                        OTH.adiquirente, 
                        OTH.banco, 
                        OTH.conta,
                        oth.operating_unit,
                        oth.header_id,
                        Sum (OTH.valor_Liquido) Valor_Liquido
                from
                ( 
                select  lin.data_Movimento, 
                        decode( upper(lin.adiquirente), 'AMEX', 'CIELO',upper(lin.adiquirente))  adiquirente, 
                        lin.banco, 
                        lpad(lin.conta,20,'0') conta,
                        ood.operating_unit,
                        hea.header_id,
                        Lin.valor_Liquido
                  from xxven_ar_rec_headers hea
                     , xxven_ar_rec_lines   lin
                     , org_organization_definitions ood
                 where hea.header_id = lin.header_id
                   and hea.status = 'U'
                   and lin.status != 'P' 
                   and ood.organization_id = lin.organization_id
                   and lin.tipo_movimento in('H2')
                   and lin.credito_debito = 'C'
                   and lin.data_movimento >= to_date('01/01/19','dd/mm/yy')--between to_date('02/07/18','dd/mm/yy') and to_date('06/07/18','dd/mm/yy')
                   ) OTH
                group by OTH.data_Movimento, OTH.adiquirente, OTH.banco, OTH.conta, oth.operating_unit,oth.header_id  

                  ) loop

      -- cliente 
      w_customer_id := null;
      if r_rec.adiquirente = 'CIELO' then
        w_customer_id := 10109;
      elsif r_rec.adiquirente = 'AMEX' then
        w_customer_id := 11110;
      elsif r_rec.adiquirente = 'REDE' then
        w_customer_id := 11108;
      elsif r_rec.adiquirente = 'SODEXO' then
        w_customer_id := 92861;
      end if;

      -- metodo de recebimento
      w_receipt_method_id := null;
      if GetContaBancaria(r_rec.banco, r_rec.conta, w_customer_id, w_receipt_method_id) then

        --trader
        w_cr_id := null;
        w_msg   := null;
        criar_recebimento(r_rec.valor_liquido   -- p_amount            in number
                        , r_rec.receipt_number  -- p_receipt_number    in varchar2
                        , r_rec.data_movimento  -- p_receipt_date      in date
                        , w_customer_id         --p_customer_id       in number
                        , w_receipt_method_id   --in number
                        , r_rec.operating_unit  --p_org_id            in number
                        , 'EQUALS'              -- p_comments          in varchar2
                        , w_msg
                        , w_cr_id);            

        if w_cr_id is null then
          dbms_output.put_line('Não criou o recebimento: ' || r_rec.receipt_number);

          fnd_file.put_line(fnd_file.log,'Não criou o recebimento: ' || r_rec.receipt_number);
          w_msg := substr( 'Receipt number: ' || r_rec.receipt_number || ' ' || w_msg,1,240);

          atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 3);
        else
          dbms_output.put_line('criou o recebimento: ' || w_cr_id);

          w_msg := ('Recebimento criado: ' || r_rec.receipt_number);
          atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 3);

          --aplicar_recebimento(w_cr_id, r_rec.receipt_number); 

          fnd_file.put_line(fnd_file.log, w_msg);
          --aplicar_recebimento(589848, r_rec.receipt_number);
        end if;  
      else
        w_cr_id := null;
        w_msg := 'Não foi possível localizar o método de recebimento para a Conta ' || lpad(r_rec.conta,20,'0')  || ' Cliente ' || r_rec.adiquirente; 
        fnd_file.put_line(fnd_file.log,w_msg);
        atualiza_tabela_auxiliar(r_rec.receipt_number, r_rec.header_id, w_cr_id, w_msg, 1);
      end if;

      commit;
      --rollback;
    end loop; 

    -- Atividades
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------

    fnd_file.put_line(fnd_file.log,' ');
    fnd_file.put_line(fnd_file.log,'Criação de atividades.');

    for r_atividades in(select SUBSTR(( substr(x.adiquirente, 1, 1) || '_' || x.tipo_Movimento || '_' || to_char(x.data_movimento,'ddmmyy') || '_' || substr(lpad( x.estabelecimento,12,'0'),-12,12) || '_' || x.banco ),1,30)RECEIPT_NUMBER
                             , x.estabelecimento, x.data_movimento, x.adiquirente, x.organization_id, x.tipo_movimento
                             , x.banco, x.conta, x.header_id, sum(x.valor_liquido) valor_liquido 
                         from (
                        select acl.estabelecimento, acl.data_movimento, acl.organization_id, substr(acl.tipo_movimento,1,1) tipo_movimento, acl.credito_debito
                             , decode( upper(acl.adiquirente), 'AMEX', 'CIELO',upper(acl.adiquirente))  adiquirente, acl.banco, lpad(acl.conta,20,'0') conta, acl.header_id
                             , decode(acl.credito_debito,'C', acl.valor_liquido, 'D', -1 * acl.valor_liquido) valor_liquido

                          from xxven_ar_rec_lines          acl 
                            , org_organization_definitions ood
                         where acl.tipo_movimento in('E1','E2','E3'
                                                    ,'F1','F2','F3'
                                                    ,'G1') 
                           and acl.status != 'P'      
                           and acl.data_movimento >= to_date('01/01/19','dd/mm/yy') --between to_date('02/07/18','dd/mm/yy') and to_date('06/07/18','dd/mm/yy')
                           --and acl.estabelecimento = 1046570886
                           and acl.organization_id = ood.organization_id
                        union all
                        select acl.estabelecimento, acl.data_movimento, acl.organization_id, substr('E',1,1) tipo_movimento, acl.credito_debito
                             , decode( upper(acl.adiquirente), 'AMEX', 'CIELO',upper(acl.adiquirente))  adiquirente, acl.banco, lpad(acl.conta,20,'0') conta, acl.header_id
                             , decode(acl.credito_debito,'C', acl.valor_liquido, 'D', -1 * acl.valor_liquido) valor_liquido

                          from xxven_ar_rec_lines          acl 
                            , org_organization_definitions ood
                         where acl.tipo_movimento in('H2') 
                           and acl.status != 'P' 
                           and acl.credito_debito = 'D'
                           and acl.data_movimento >= to_date('01/01/19','dd/mm/yy') --between to_date('02/07/18','dd/mm/yy') and to_date('06/07/18','dd/mm/yy')
                           and acl.organization_id = ood.organization_id)x 
                        group by x.estabelecimento, x.data_movimento, x.adiquirente, x.organization_id, x.tipo_movimento, x.banco, x.conta , x.header_id
                        order by x.estabelecimento, x.data_movimento, x.adiquirente, x.organization_id
                                                    ) loop
      w_cr_id             := null;
      w_receipt_method_id := null;

      w_customer_id := null;
      if r_atividades.adiquirente = 'CIELO' then
        w_customer_id := 10109;
      elsif r_atividades.adiquirente = 'AMEX' then
        w_customer_id := 11110;
      elsif r_atividades.adiquirente = 'REDE' then
        w_customer_id := 11108;
      elsif r_atividades.adiquirente = 'SODEXO' then
        w_customer_id := 92861;
      end if;

      if GetContaBancaria(r_atividades.banco, r_atividades.conta, w_customer_id, w_receipt_method_id) and 
         GetAtividade(r_atividades.estabelecimento, r_atividades.tipo_movimento, r_atividades.adiquirente, w_activity, w_erro) then  

        AR_RECEIPT_API_PUB.CREATE_MISC( p_api_version       => 1.0,
                                        p_init_msg_list     => FND_API.G_TRUE,
                                        p_commit            => FND_API.G_TRUE,
                                        p_validation_level  => FND_API.G_VALID_LEVEL_FULL,
                                        x_return_status     => l_return_status,
                                        x_msg_count         => l_msg_count,
                                        x_msg_data          => l_msg_data,
                                        p_amount            => r_atividades.valor_liquido,
                                        p_receipt_date      => r_atividades.data_movimento, --TO_DATE('02/07/18','DD/MM/YY'),
                                        p_gl_date           => r_atividades.data_movimento,
                                        p_receipt_method_id => w_receipt_method_id, --*** 2133,
                                        p_comments          => 'EQUALS',
                                        p_activity          => w_activity,                  
                                        p_misc_receipt_id   => w_cr_id ,
                                        p_receipt_number    => r_atividades.receipt_number);


        fnd_file.put_line(fnd_file.log,'w_cr_id: ' || w_cr_id);

        if w_cr_id > 0 then
          w_msg :=  'Recebimento (Atividade) criado: ' || r_atividades.receipt_number;
          atualiza_tabela_auxiliar(r_atividades.receipt_number, r_atividades.header_id, w_cr_id, w_msg, 2);

          fnd_file.put_line(fnd_file.log,w_msg);
        else
          w_msg :=  'Recebimento (Atividade) erro: ' || r_atividades.receipt_number;
          atualiza_tabela_auxiliar(r_atividades.receipt_number, r_atividades.header_id, w_cr_id, w_msg, 2);
          fnd_file.put_line(fnd_file.log,w_msg);
        end if;

        IF l_msg_count = 1 THEN
          fnd_file.put_line(fnd_file.log, 'l_msg_data ' || l_msg_data);
        ELSIF l_msg_count > 1 THEN
          w_count := 0;

          LOOP
            w_count := w_count + 1;
            l_msg_data := fnd_msg_pub.get (fnd_msg_pub.g_next, fnd_api.g_false);

            IF l_msg_data IS NULL THEN
               EXIT;
            END IF;

            fnd_file.put_line(fnd_file.log, 'Message' || w_count || ' ---' || l_msg_data);

          end loop;
        end if;  
      else
        w_msg := 'Erro ao tentar encontrar o método de recebimento ou Atividade Micsc: ' || w_erro;
        atualiza_tabela_auxiliar(r_atividades.receipt_number, r_atividades.header_id, w_cr_id, w_msg, 2);
        fnd_file.put_line(fnd_file.log, w_msg);
      END IF;

      commit;
    end loop;


  end;

  function GetAtividade(p_estabelecimento in varchar2
                       , p_tipo_movimento in varchar2
                       , p_adiquirente    in varchar2
                       , p_activity      out varchar2
                       , p_erro          out varchar2) return boolean as
  begin
    select name into p_activity
      from ar_receivables_trx_all
     where type = 'MISCCASH'
       and attribute3 = p_adiquirente
       and attribute4 = p_estabelecimento 
       and attribute5 = p_tipo_movimento;

    return true;   
  exception
    when others then
      p_erro := sqlerrm;
      fnd_file.put_line(fnd_file.log,'attribute3: ' || p_adiquirente);
      fnd_file.put_line(fnd_file.log,'attribute4: ' || p_estabelecimento);
      fnd_file.put_line(fnd_file.log,'attribute5: ' || p_tipo_movimento);

      fnd_file.put_line(fnd_file.log,'Atividade não localizada.' );
      return false;
  end;

  procedure Set_Atualiza_Parcela(p_lote_unico       in varchar2
                               , p_header_id       in number
                               , p_tipo_movimento  in varchar2
                               , p_customer_trx_id in number) as
  begin
    for r_parc in(select a.lote_unico, to_date(a.data_venc_parcela,'YYYYMMDD') venc, a.parcela, a.customer_trx_id, a.valor_liquido
                         , (select sum(valor_liquido) 
                              from xxven_ar_mov_lines b
                             where b.lote_unico = a.lote_unico
                               and b.tipo_movimento = a.tipo_movimento
                               and b.header_id = a.header_id
                               and b.parcela = a.parcela) sum_valor_liquido
                    from xxven_ar_mov_lines a
                 where 1=1
                   and lote_unico = p_lote_unico
                   and tipo_movimento = p_tipo_movimento
                   and header_id = p_header_id
    /*
                 select lote_unico, to_date(data_venc_parcela,'YYYYMMDD') venc, parcela, customer_trx_id, valor_liquido
                  from xxven_ar_mov_lines
                 where 1=1
                   --and customer_trx_id =  19121887 
                   and lote_unico = p_lote_unico
                   and tipo_movimento = p_tipo_movimento
                   and header_id = p_header_id */
                   ) loop
      update ar_payment_schedules_all set due_date = r_parc.venc, amount_due_original         = r_parc.sum_valor_liquido
                                                                , amount_due_remaining        = r_parc.sum_valor_liquido
                                                                , AMOUNT_LINE_ITEMS_REMAINING = r_parc.sum_valor_liquido
                                                                , AMOUNT_LINE_ITEMS_ORIGINAL  = r_parc.sum_valor_liquido
                                                                , ACCTD_AMOUNT_DUE_REMAINING  = r_parc.sum_valor_liquido

       where customer_trx_id = p_customer_trx_id 
         and to_char(terms_sequence_number) = r_parc.parcela;
    end loop;
  end;

  FUNCTION get_terms(
                       p_lote_unico  IN  VARCHAR2
                     , p_qtd         IN  NUMBER
                     , p_terms_id    OUT NUMBER
                     , p_adiquirente IN  NUMBER -- ASChaves 20190130 - Identificar o cliente
                     , p_org_id      IN  NUMBER
                    )
  RETURN BOOLEAN AS
    lv_adiquirente  VARCHAR2(5000);
    lv_pre          VARCHAR2(2);
  BEGIN
    dbms_output.put_line('qtd parcela ' || p_qtd);
    --
    IF p_adiquirente = '1' THEN
      lv_adiquirente := 'CIELO';
    ELSIF p_adiquirente = '2' THEN
      lv_adiquirente := 'REDE';
    ELSIF p_adiquirente = '4' THEN
      lv_adiquirente := 'TEMPO'; 
    ELSIF p_adiquirente = '12' THEN  
      lv_adiquirente := 'SODEXO'; 
    END IF;
    --
	IF p_org_id = 83 THEN
      lv_pre := 'DV';
	ELSE
      lv_pre := 'HN';
    END IF;
    --
    BEGIN
      SELECT   rt.term_id
        INTO   p_terms_id
        FROM   ra_terms rt
      WHERE 1=1
        AND rt.end_date_active IS NULL
        AND rt.name            LIKE (lv_pre||'%_EQ_'||lv_adiquirente||'%')
        AND TO_NUMBER(TRIM(SUBSTR(rt.name, ((LENGTH(rt.name) - INSTR(rt.name, '_',-1))*-1), (INSTR(rt.name, 'X',-1) - (INSTR(rt.name, '_',-1) +1))))) = p_qtd
      ;
      RETURN TRUE;
    EXCEPTION
      WHEN OTHERS THEN
         fnd_file.put_line(fnd_file.log,'Terms: não encontrado.');
	     RETURN FALSE;
    END;

    -- Codigo Descontinuado --
    -- ASChaves 20190130 - begin
    -- ASChaves 20190130 -   select terms.term_id
    -- ASChaves 20190130 -     into p_terms_id
    -- ASChaves 20190130 -     from (select ra.term_id, (select count(term_id) from ra_terms_lines where term_id = ra.term_id) qtd
    -- ASChaves 20190130 -             from ra_terms    ra
    -- ASChaves 20190130 -            where ra.end_date_active is null) terms
    -- ASChaves 20190130 -    where terms.qtd = p_qtd
    -- ASChaves 20190130 -      and rownum = 1;
    -- ASChaves 20190130 - 
    -- ASChaves 20190130 -   return true;
    -- ASChaves 20190130 - exception
    -- ASChaves 20190130 -   when others then
    -- ASChaves 20190130 -     dbms_output.put_line('Terms: não encontrado.');
    -- ASChaves 20190130 -     return false;
    -- ASChaves 20190130 - end;
  END get_terms;

  function file_exist(p_file in varchar2) return boolean as
    i number := 0;
  begin
    begin
      select count(header_id)
        into i
        from xxven_ar_mov_header
       where file_name = p_file;

      if i > 0 then
        return true;
      else
        return false;
      end if;
    exception
      when others then
        return false;
    end;
  end;

  function file_exist_rec(p_file in varchar2) return boolean as
    i number := 0;
  begin
    begin
      select count(header_id)
        into i
        from xxven_ar_rec_headers
       where file_name = p_file;

      if i > 0 then
        return true;
      else
        return false;
      end if;
    exception
      when others then
        return false;
    end;
  end;

  function ValidacaoRegistroExistente(p_estabelecimento in varchar2
                                    , p_adiquirente     in varchar2
                                    , p_data_movimento  in date
                                    , p_lote_unico      in varchar2
                                    , p_parcela         in number
                                    , p_valor_bruto     in number ) return boolean as
    w_count number;
  begin
    begin
      select 1 into w_count
        from xxven_ar_mov_lines  
       where estabelecimento = p_estabelecimento
         and adiquirente     = p_adiquirente
         and data_movimento  = p_data_movimento
         and lote_unico      = p_lote_unico
         and parcela         = p_parcela
         and valor_bruto     = p_valor_bruto;

      return true;
    exception
      when others then
        return false;
    end;
  end;

  function VerificaSeExisteRemessa(p_id_remessa in number, p_tipo in number) return boolean as
    v_remessa varchar2(1);
  begin
    if p_tipo = 1 then
        begin
          select '1' into v_remessa
            from xxven_ar_mov_header
           where id_remessa = p_id_remessa
             and rownum = 1;
          return false;
        exception
          when no_data_found then
            return true;
        end;
    elsif p_tipo = 2 then
      begin
          select '1' into v_remessa
            from xxven_ar_rec_headers
           where id_remessa = p_id_remessa
             and rownum = 1;
          return false;
        exception
          when no_data_found then
            return true;
        end;
    end if;
  end;

  procedure importar_arquivo_mov_venda (errbuf    out varchar2
                                       ,retcode   out number)as
    type rec_csv_column_mov is record(estabelecimento   varchar2(100)
                                  , adiquirente       varchar2(100)
                                  , filial            varchar2(100)
                                  , tipo_movimento    varchar2(100)
                                  , data_movimento    date
                                  , lote_unico        varchar2(100)
                                  , organization_id   number
                                  , parcela           number
                                  , valor_bruto       number
                                  , valor_comissao    number
                                  , valor_liquido     number
                                  , produto           varchar2(10)
                                  , bandeira          varchar2(100)
                                  , data_venc_parcela varchar2(8)
                                  , status            varchar2(1));
    type rec_type_record is table of rec_csv_column_mov index by binary_integer;
    rec_movimento rec_type_record;

    v_dir         varchar2(100);
    file_error    number := 0;
    file_handle   utl_file.file_type;
    w_texto       varchar2(1000);
    j             number;
    i             number;
    w_adiquirente varchar2(100);
    w_existe      varchar2(1);
    w_path        varchar2(150);
    w_id_remessa number;
    w_header_id   number;

    function get_line_csv(p_text in varchar2
                      , p_ini  in number
                      , p_fim  in number) return string is
    begin
      if p_fim != 32767 then
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, instr(p_text,';',1, p_fim) - instr(p_text,';',1, p_ini) - 1));
      else
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, p_fim));
      end if;
    exception
      when others then
        return '';
    end get_line_csv;

  begin
    v_dir := 'EQUALS_MOVIMENTACAO';

    begin
      select directory_path
        into w_path
        from all_directories
       where directory_name = v_dir;
    end;

    fnd_file.put_line(fnd_file.output,'=========================================================');
    fnd_file.put_line(fnd_file.output,'**************** INICIO DO PROCESSAMENTO ****************');
    fnd_file.put_line(fnd_file.output,'=========================================================');

    for r_file in (select column_value as arquivo
                     from table(apps.xmlcsv_lista_arquivo(w_path))
                    where column_value like '%.csv') loop

      fnd_file.put_line(fnd_file.output,'Arquivo sendo processado: ' || r_file.arquivo);
      dbms_output.put_line('Arquivo sendo processado: ' || r_file.arquivo);
      w_id_remessa := 0;
      begin
        select '1'
          into w_existe
          from xxven_ar_mov_header
         where file_name = r_file.arquivo
           and rownum = 1;

        fnd_file.put_line(fnd_file.output,'Arquivo já importado: ' || r_file.arquivo);
        dbms_output.put_line('Arquivo já importado: ' || r_file.arquivo);

        exit;
      exception
        when others then
          null;
          --fnd_file.put_line(fnd_file.output,'Arquivo já importado: ' || SQLERRM);
      end;

      select xxven_ar_conc_cc_headers_s.nextval into w_header_id from dual;

      insert into xxven_ar_mov_header(header_id
                                    , process_date
                                    , status
                                    , file_name
                                    , creation_date
                                    , created_by)
                               values(w_header_id --xxven_ar_conc_cc_headers_s.nextval
                                    , sysdate
                                    , 'U'
                                    , r_file.arquivo
                                    , sysdate
                                    , -1 );

      begin

        file_handle := utl_file.fopen(v_dir,r_file.arquivo, 'R',2000);

        begin
          utl_file.get_line(file_handle,w_texto);
          utl_file.get_line(file_handle, w_texto);
          dbms_output.put_line('texto: ' || w_texto);

          w_id_remessa := substr(w_texto,length(w_texto)-15,16);
          fnd_file.put_line(fnd_file.output,'texto ' || w_texto); 
          fnd_file.put_line(fnd_file.output,'ID_REMESSA: ' || substr(w_texto,length(w_texto)-15,16) ); 
        exception
          when others then
            fnd_file.put_line(fnd_file.output,'Erro ao ler primeira linha');
            dbms_output.put_line('Erro ao ler primeira linha');
        end;

      exception
        when utl_file.invalid_operation then
          fnd_file.put_line(fnd_file.output,'Operação inválida no arquivo.');
          file_error := 1;
          dbms_output.put_line('Operação inválida no arquivo.');

        when utl_file.invalid_path then
          fnd_file.put_line(fnd_file.output,'Diretório inválido.');
          file_error := 1;
          dbms_output.put_line('Diretório inválido.');

        when others then
          file_error := 1;
          fnd_file.put_line(fnd_file.output,'erro primeira linha');
          dbms_output.put_line('erro primeira linha');

       end;

      if utl_file.is_open(file_handle) and (file_error = 0) and VerificaSeExisteRemessa(w_id_remessa, 1) then

        update xxven_ar_mov_header set id_remessa = w_id_remessa 
         where header_id = w_header_id; 

        rec_movimento.delete;
        j := 0;
        i := 0;

        begin
          loop
            if j != 0 then
              utl_file.get_line(file_handle, w_texto);
            end if;  

            j := j + 1;

            w_texto := replace(w_texto,CHR(10),'');

            rec_movimento(j).status := 'P';

            rec_movimento(j).lote_unico := get_line_csv(w_texto, 10 ,11);
            --dbms_output.put_line('Lote unico: ' || rec_movimento(j).lote_unico);

            rec_movimento(j).parcela := get_line_csv(w_texto, 11 ,12);
            --dbms_output.put_line('Parcela: ' || rec_movimento(j).parcela);

            rec_movimento(j).bandeira := get_line_csv(w_texto, 20 ,21);
            --dbms_output.put_line('Bandeira: ' || rec_movimento(j).bandeira);

            rec_movimento(j).produto := get_line_csv(w_texto, 21 ,22);

            rec_movimento(j).adiquirente := get_line_csv(w_texto, 4 ,5);
            w_adiquirente                := upper(rec_movimento(j).adiquirente);
            --dbms_output.put_line('Adiquirente: ' || w_adiquirente);

            rec_movimento(j).tipo_movimento := get_line_csv(w_texto, 6 ,7);

            rec_movimento(j).estabelecimento := get_line_csv(w_texto, 1 ,2);
            --dbms_output.put_line('Estabelecimento: ' || rec_movimento(j).estabelecimento);

            begin
              if upper(w_adiquirente) = '2'     then
                w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_REDE';
              elsif upper(w_adiquirente) = '4'  then
                w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_AMEX';
              elsif upper(w_adiquirente) = '1' then
                w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_CIELO';
              elsif upper(w_adiquirente) = '12' then
                w_adiquirente := 'XXVEN_ESTABELE_FILIAL_C_SODEXO';
              end if;

              begin
              rec_movimento(j).data_movimento := to_date(get_line_csv(w_texto, 8 ,9), 'YYYYMMDD');
              --dbms_output.put_line('Data Movimento: ' || rec_movimento(j).data_movimento);

              rec_movimento(j).data_venc_parcela := get_line_csv(w_texto, 33 ,34);
              --dbms_output.put_line('Data venc parcela: ' || to_date(rec_movimento(j).data_venc_parcela, 'YYYYMMDD')) ;

            exception
              when others then
                rec_movimento(j).status := 'E';
            end;

              --dbms_output.put_line('w_adiquirente: ' || w_adiquirente);

              SELECT description
                   , (select hr.organization_id from hr_all_organization_units hr where hr.name = description)
                INTO rec_movimento(j).filial
                   , rec_movimento(j).organization_id
                FROM fnd_lookup_values
               WHERE lookup_type     = w_adiquirente ---'XXVEN_ESTABELE_FILIAL_CC_AMEX'
                 AND (attribute7     = rec_movimento(j).estabelecimento or attribute7     = ('00' || rec_movimento(j).estabelecimento) ) 
                 AND LANGUAGE        = 'PTB' --USERENV('LANG')
                 AND ( nvl(end_date_active, trunc(sysdate)) >=  rec_movimento(j).data_movimento and start_date_active <= rec_movimento(j).data_movimento) --trunc(sysdate)
                 AND enabled_flag    = 'Y';

              --dbms_output.put_line('Filial: ' || rec_movimento(j).filial);
              --dbms_output.put_line('Organization: ' || rec_movimento(j).organization_id);
            exception
              when others then
                rec_movimento(j).filial := null;
            end;



            rec_movimento(j).valor_bruto := to_number(replace(get_line_csv(w_texto, 16 ,17),',','.'),'9999999999.99');
            --dbms_output.put_line('Valor bruto: ' || rec_movimento(j).valor_bruto);

            rec_movimento(j).valor_comissao := to_number(replace(get_line_csv(w_texto, 17 ,18),',','.'),'9999999999.99');
            --dbms_output.put_line('Valor comissao: ' || rec_movimento(j).valor_comissao);

            rec_movimento(j).valor_liquido := to_number(replace(get_line_csv(w_texto, 18 ,19),',','.'),'9999999999.99');
            --dbms_output.put_line('Valor liquido: ' || rec_movimento(j).valor_liquido);
            --if not ValidacaoRegistroExistente(rec_movimento(j).estabelecimento
            --                                , rec_movimento(j).adiquirente
            --                                , rec_movimento(j).data_movimento
            --                                , rec_movimento(j).lote_unico
            --                                , rec_movimento(j).parcela
            --                                , rec_movimento(j).valor_bruto)       then

            insert into xxven_ar_mov_lines(line_id
                                         , header_id
                                         , estabelecimento
                                         , adiquirente
                                         , filial
                                         , tipo_movimento
                                         , data_movimento
                                         , lote_unico
                                         , parcela
                                         , valor_bruto
                                         , valor_comissao
                                         , valor_liquido
                                         , bandeira
                                         , produto
                                         , data_venc_parcela
                                         , status
                                         , organization_id
                                         , desc_status
                                           )
                                    values(xxven_ar_conc_cc_lines_s.nextval
                                         , xxven_ar_conc_cc_headers_s.currval
                                         , rec_movimento(j).estabelecimento
                                         , rec_movimento(j).adiquirente
                                         , rec_movimento(j).filial
                                         , rec_movimento(j).tipo_movimento
                                         , rec_movimento(j).data_movimento
                                         , rec_movimento(j).lote_unico
                                         , rec_movimento(j).parcela
                                         , rec_movimento(j).valor_bruto
                                         , rec_movimento(j).valor_comissao
                                         , rec_movimento(j).valor_liquido
                                         , rec_movimento(j).bandeira
                                         , rec_movimento(j).produto
                                         , rec_movimento(j).data_venc_parcela
                                         , 'U' --rec_movimento(j).status
                                         , rec_movimento(j).organization_id
                                        , null
                                        );
              i := i + 1;                            
            --else
            --  fnd_file.put_line(fnd_file.output,'Registro já importado.' );
            --  fnd_file.put_line(fnd_file.output,'Estabelecimento:' || rec_movimento(j).estabelecimento);
            --  fnd_file.put_line(fnd_file.output,'Adquirente: '     ||  rec_movimento(j).adiquirente);
            --  fnd_file.put_line(fnd_file.output,'Data movimento: ' || rec_movimento(j).data_movimento);
            --  fnd_file.put_line(fnd_file.output,'Lote unico: '     || rec_movimento(j).lote_unico);
            --  fnd_file.put_line(fnd_file.output,'Parcela: '        || rec_movimento(j).parcela);
            --  fnd_file.put_line(fnd_file.output,'Valor bruto: '    || rec_movimento(j).valor_bruto);
            --end if;

            commit;

          end loop;

        exception
          when others then
            dbms_output.put_line('Linhas: ' || i);
            fnd_file.put_line(fnd_file.output,'Linhas importadas: ' || i);
            fnd_file.put_line(fnd_file.output,w_texto);
            fnd_file.put_line(fnd_file.output,'erro primeira linha: ' || sqlerrm);
            rollback;
        end;
        utl_file.fclose(file_handle);
      end if;
    end loop;
  end importar_arquivo_mov_venda;

  procedure processar_dinheiro(errbuf    out varchar2
                              ,retcode   out number
                              ,p_data_ini in varchar2
                              ,p_data_fim in varchar2) as

    CURSOR list_errors IS
      SELECT trx_header_id
           , trx_line_id
           , trx_salescredit_id
           , trx_dist_id
           , trx_contingency_id
           , error_message
           , invalid_value
        FROM ar_trx_errors_gt;

    v_ship_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_bill_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_account_number      hz_cust_accounts.account_number%type;
    v_cust_account_id     hz_cust_accounts.cust_account_id%type;
    v_term_id             number;
    l_batch_source_id     ra_batch_sources_all.batch_source_id%type;
    l_cust_acct_site_id   hz_cust_acct_sites_all.cust_acct_site_id%TYPE;
    l_ship_to_site_use_id hz_cust_site_uses_all.site_use_id%TYPE;
    l_ship_to_address_id  number;
    w_cust_trx_type_id    ra_cust_trx_types_all.cust_trx_type_id%type;
    l_set_of_book_id      number;
    w_batch_source_id     number;

    l_trx_header_id        number;
    l_trx_line_id          number;
    l_batch_source_rec     ar_invoice_api_pub.batch_source_rec_type;
    l_trx_header_tbl       ar_invoice_api_pub.trx_header_tbl_type;
    l_trx_lines_tbl        ar_invoice_api_pub.trx_line_tbl_type;
    l_trx_dist_tbl         ar_invoice_api_pub.trx_dist_tbl_type;
    l_trx_salescredits_tbl ar_invoice_api_pub.trx_salescredits_tbl_type;
    l_customer_trx_id      apps.ra_customer_trx_all.customer_trx_id%type;
    l_rErrorlist           varchar2(4000);
    w_erro                 varchar2(4000);

    l_return_status VARCHAR2(500);
    l_msg_count     number;
    l_msg_data      VARCHAR2(4000);
    l_msg_error     VARCHAR2(10000);
    w_organizacao_venda varchar2(3);

    ln_cnt                   PLS_INTEGER := 0;

  begin
    fnd_file.put_line(fnd_file.log,' ');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'************ INICIO DO PROCESSAMENTO DINHEIRO ***********');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,' ');

    for r_mov in (select msi.inventory_item_id item_id, msi.segment1, msi.global_attribute3 origem, msi.description item_descricao, msi.primary_uom_code uom_code
                        , msi.global_attribute2, msi.global_attribute4 tipo_fiscal, msi.global_attribute5 sit_federal, msi.global_attribute6 sit_estadual
                        , lpad(cab.organizacao_venda,3,0) organizacao_venda, ood.organization_id,ood.operating_unit, cab.data_hora, pag.tipo_pagamento, sum(pag.valor_pago) valor_item
                     from mtl_system_items_b                  msi
                        , tb_anali_ebs_ped_venda_pagam@intprd pag
                        , tb_anali_ebs_ped_venda_cab@intprd   cab
                        , org_organization_definitions        ood
                    where msi.segment1 = '67247'
                      and msi.organization_id = 174
                      and ood.organization_code = lpad(cab.organizacao_venda,3,0)
                      and cab.id_sequencial     = pag.id_ped_venda_cab
                      and pag.envio_status is null
                      --and cab.envio_status = 40
                      and cab.tipo_ordem = 1
                      and cab.data_hora between nvl( to_date(to_date(p_data_ini ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                                            and nvl( to_date(to_date(p_data_fim ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                      and pag.tipo_pagamento in('0')
                 group by msi.inventory_item_id , msi.segment1, msi.global_attribute3, msi.description, msi.primary_uom_code
                        , msi.global_attribute2, msi.global_attribute4, msi.global_attribute5 , msi.global_attribute6
                        , cab.organizacao_venda, ood.organization_id,ood.operating_unit, cab.data_hora, pag.tipo_pagamento
                 order by cab.organizacao_venda,cab.data_hora, pag.tipo_pagamento) loop

      w_organizacao_venda := null;

      if (r_mov.data_hora >= to_date('01/05/19','dd/mm/yy')) and (r_mov.organizacao_venda = '015') then 
        w_organizacao_venda := '515';
      elsif (r_mov.data_hora >= to_date('01/05/19','dd/mm/yy')) and (r_mov.organizacao_venda = '055') then 
        w_organizacao_venda := '555';
      else
        w_organizacao_venda := r_mov.organizacao_venda;
      end if;

      if valida_cliente(0, w_organizacao_venda, r_mov.operating_unit, v_ship_to, v_bill_to, v_account_number, v_cust_account_id) then
        ---- cliente --
        BEGIN
          SELECT cust_acct_site_id
            INTO l_cust_acct_site_id
            FROM hz_cust_acct_sites_all
           WHERE cust_account_id = v_cust_account_id --l_cust_account_id
             AND status          = 'A'
             AND org_id          = FND_GLOBAL.ORG_ID;

        EXCEPTION
          WHEN OTHERS THEN

            dbms_output.put_line('não encontrou l_cust_acct_site_id');
            fnd_file.put_line(fnd_file.log, 'não encontrou l_cust_acct_site_id');

        END;

        begin
          SELECT site_use_id
            INTO l_ship_to_site_use_id
            FROM hz_cust_site_uses_all
           WHERE cust_acct_site_id = l_cust_acct_site_id
             AND status            = 'A'
             AND site_use_code     = 'SHIP_TO'
             AND org_id            = FND_GLOBAL.ORG_ID;
        exception
          when others then
            dbms_output.put_line('não encontrou l_ship_to_site_use_id');
            fnd_file.put_line(fnd_file.log, 'não encontrou l_ship_to_site_use_id');  
            l_ship_to_site_use_id := null;
        end;     

        BEGIN
          SELECT cust_acct_site_id
            INTO l_ship_to_address_id
            FROM hz_cust_site_uses_all
           WHERE site_use_id = l_ship_to_site_use_id;
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line('nao encontrado l_ship_to_address_id');
            fnd_file.put_line(fnd_file.log,'nao encontrador l_ship_to_address_id' );
            l_ship_to_address_id := null;
        END;

        v_term_id := 1024; -- condição de pagamento dinheiro

        begin
          select rct.cust_trx_type_id  into w_cust_trx_type_id
            from ra_cust_trx_types_all rct
           where rct.org_id = r_mov.operating_unit
             and name   = '5102_5405_ANALISA';
        exception
          when no_data_found then
            w_cust_trx_type_id := 0;
        end;

        begin
          select batch_source_id into w_batch_source_id
           from ra_batch_sources_all
          where org_id                            = r_mov.operating_unit
            and attribute1                        = w_organizacao_venda
            and description                       like '%_N'
            and status                            = 'A'
            and batch_source_type                 = 'INV'
            and end_date                          is null
            and nvl(global_attribute5,'N')         = 'N'
           and nvl(auto_trx_numbering_flag, 'N') = 'Y';
        exception
          when others then
            w_batch_source_id := null;
            dbms_output.put_line(' Não encontrou w_batch_source_id ' || w_organizacao_venda);
        end;

        if (l_ship_to_address_id is not null) and (l_ship_to_site_use_id is not null) and (l_cust_acct_site_id is not null) then

          SELECT ra_customer_trx_s.nextval
            INTO l_trx_header_id
            FROM dual;

          ---------------
          -- cabeçalho
          l_set_of_book_id                                   := fnd_profile.value('GL_SET_OF_BKS_ID');
          l_trx_header_tbl(1).trx_header_id                  := l_trx_header_id;
          --l_trx_header_tbl(1).trx_number                     := (r_mov.organizacao_venda || to_char(r_mov.data_hora,'DDMMYY') || 'T2');
          l_trx_header_tbl(1).interface_header_context       := 'EQUALS'; --  r_mov.ct_reference; 
          --
          l_trx_header_tbl(1).ship_to_site_use_id            := l_ship_to_site_use_id; --p_ship_to_site_use_id;
          l_trx_header_tbl(1).ship_to_address_id             := l_ship_to_address_id; --l_ship_to_address_id;
          l_trx_header_tbl(1).bill_to_customer_id            := v_cust_account_id; --p_customer_id;
          l_trx_header_tbl(1).ship_to_customer_id            := v_cust_account_id; --p_customer_id;
          l_trx_header_tbl(1).sold_to_customer_id            := v_cust_account_id; --p_customer_id;
          --
          l_trx_header_tbl(1).term_id                        := v_term_id; --p_term_id;
          l_trx_header_tbl(1).cust_trx_type_id               := w_cust_trx_type_id; -- p_cust_trx_type_id;
          l_trx_header_tbl(1).printing_option                := 'NOT';
          l_trx_header_tbl(1).status_trx                     := 'OP';
          l_trx_header_tbl(1).trx_date                       := trunc(r_mov.data_hora); --P_TRX_DATE;
          --l_trx_header_tbl(1).GL_date                        := trunc(r_mov.data_hora); --P_GL_DATE;
          l_trx_header_tbl(1).trx_currency                   := 'BRL';
          --l_trx_header_tbl(1).attribute11                    := r_mov.lote_unico;
          l_trx_header_tbl(1).global_attribute_category      := 'JL.BR.ARXTWMAI.Additional Info';
          l_trx_header_tbl(1).org_id                         := r_mov.operating_unit; --l_nOrg_id;

            ---------------------------------------
            -- Populate batch source information --
            ---------------------------------------
            l_batch_source_rec.batch_source_id                 := w_batch_source_id;
            --l_batch_source_rec.default_date                    := trunc(sysdate);
            ---------------------------------
            -- Populate line 1 information --
            ---------------------------------
            BEGIN
              SELECT ra_customer_trx_lines_s.nextval
                INTO l_trx_line_id
                FROM dual;
            END;

            l_trx_lines_tbl(1).trx_header_id                   := l_trx_header_id;
            l_trx_lines_tbl(1).trx_line_id                     := l_trx_line_id;
            l_trx_lines_tbl(1).line_number                     := 1;
            l_trx_lines_tbl(1).quantity_invoiced               := 1;
            l_trx_lines_tbl(1).unit_selling_price              := r_mov.valor_item;
            l_trx_lines_tbl(1).line_type                       := 'LINE';
            l_trx_lines_tbl(1).warehouse_id                    := r_mov.organization_id;
            l_trx_lines_tbl(1).inventory_item_id               := r_mov.item_id;
            l_trx_lines_tbl(1).uom_code                        := r_mov.uom_code;
            l_trx_lines_tbl(1).global_attribute_category       := 'JL.BR.ARXTWMAI.Additional Info';
            l_trx_lines_tbl(1).unit_standard_price             := r_mov.valor_item;
            --l_trx_lines_tbl(1).memo_line_id                  := 1006; -- p_memo_line_id;
            --l_trx_lines_tbl(1).description                   := 'CARTAO DE CREDITO';

            BEGIN
              l_customer_trx_id := null;
              AR_INVOICE_API_PUB.create_single_invoice ( p_api_version          => 1.0
                                                       , p_init_msg_list        => FND_API.G_TRUE
                                                       , p_commit               => FND_API.G_FALSE
                                                       , p_batch_source_rec     => l_batch_source_rec
                                                       , p_trx_header_tbl       => l_trx_header_tbl
                                                       , p_trx_lines_tbl        => l_trx_lines_tbl
                                                       , p_trx_dist_tbl         => l_trx_dist_tbl
                                                       , p_trx_salescredits_tbl => l_trx_salescredits_tbl
                                                       , x_customer_trx_id      => l_customer_trx_id
                                                       , x_return_status        => l_return_status
                                                       , x_msg_count            => l_msg_count
                                                       , x_msg_data             => l_msg_data);

              --dbms_output.put_line('l_customer_trx_id: ' || l_customer_trx_id);
              --dbms_output.put_line('l_return_status: '   || l_return_status);
              --dbms_output.put_line('l_msg_count: '   || l_msg_count);

              update ra_customer_trx_all set ct_reference = (w_organizacao_venda || '-' ||to_char(r_mov.data_hora,'DDMMYY') ) where customer_trx_id = l_customer_trx_id;
            exception
              when others then
                FND_FILE.PUT_LINE(FND_FILE.LOG,'Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
                dbms_output.put_line('Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
            end;
            --
            -- ASChaves  20190406 - Get API Status --
            fnd_file.put_line(fnd_file.log,'Status:        ' || l_return_status);
            fnd_file.put_line(fnd_file.log,'Message count: ' || l_msg_count);
            --
            IF l_msg_count = 1 THEN
               fnd_file.put_line(fnd_file.log,'l_msg_data  ' || l_msg_data);
            ELSIF l_msg_count > 1 THEN
               LOOP
                  ln_cnt     := ln_cnt + 1;
                  l_msg_data := fnd_msg_pub.get(fnd_msg_pub.g_next,fnd_api.g_false);
                  IF l_msg_data IS NULL THEN
                     EXIT;
                  END IF;
                  fnd_file.put_line(fnd_file.log,'Message ' || ln_cnt ||'. '||l_msg_data);
               END LOOP;
            ELSE
              fnd_file.put_line(fnd_file.log,'Message ' || ln_cnt ||'. '||l_msg_data);
            END IF;
            --
            --
            FND_FILE.PUT_LINE(FND_FILE.LOG, 'w_erro: ' || l_msg_count);
            --dbms_output.put_line('Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
            --dbms_output.put_line('l_customer_trx_id: ' || l_customer_trx_id);

            if nvl(l_customer_trx_id,0) > 0 then

              AtualizaBarramentoDinheiro(r_mov.organizacao_venda, r_mov.data_hora, l_customer_trx_id, w_erro);

            else
              w_erro := 'Erro ao criar a transação dinheiro.';
              AtualizaBarramentoDinheiro(r_mov.organizacao_venda, r_mov.data_hora, 0, w_erro);
            end if;

            commit;

            l_rErrorlist := NULL;
            FOR i IN list_errors LOOP

              l_rErrorlist := substr(i.error_message,1,80)||' - '||substr(i.invalid_value,1,80);

            END LOOP;

            IF l_return_status <> FND_API.G_RET_STS_SUCCESS OR l_rErrorlist IS NOT NULL OR l_customer_trx_id IS NULL THEN
              --
              w_erro := 'Erro-Criacao de Titulo. '||l_customer_trx_id;
              --dbms_output.put_line('Passou ');
              --dbms_output.put_line('w_erro: ' || l_msg_data);
              FND_FILE.PUT_LINE(FND_FILE.LOG, 'w_erro: ' || l_msg_data);
              --
              IF l_msg_count > 0 THEN
                --
                FOR l_msg_index in 1..l_msg_count LOOP
                  --
                  l_msg_data := FND_MSG_PUB.GET(p_msg_index =>l_msg_index,p_encoded=>'F');
                  --
                  IF l_msg_data is not null THEN
                    w_erro := SUBSTR(w_erro||' '||l_msg_data,1,200);

                    --dbms_output.put_line('w_erro: ' || w_erro);
                  END IF;
                  --
                END LOOP;
                --
              ELSE
                --
                l_msg_data := FND_MSG_PUB.GET;
                --
                IF l_msg_data IS NOT NULL THEN
                  w_erro := SUBSTR(w_erro||' '||l_msg_data,1,200);

                END IF;
                --
              END IF;
              --
              FOR C_ERRO IN (SELECT trx_header_id
                                  , trx_line_id
                                  , trx_salescredit_id
                                  , trx_dist_id
                                  , trx_contingency_id
                                  , error_message
                                  , invalid_value
                               FROM ar_trx_errors_gt)
                 LOOP
                  w_erro := SUBSTR(w_erro || substr(C_ERRO.error_message,1,80)
                                         || 'Valor invalido = ' || substr(C_ERRO.invalid_value,1,80),1,200);


              END LOOP;
              --
            END IF;
            --
            --COMMIT;
        end if;
      else
        -- tratamento quando não acha cliente
        w_erro := 'Erro ao encontrar cliente. organizacao_venda: ' || w_organizacao_venda;
        AtualizaBarramentoDinheiro(w_organizacao_venda, r_mov.data_hora, 0, w_erro);
      end if;

    end loop;
  end processar_dinheiro;

  procedure AtualizaBarramentoDinheiro(p_organizacao_venda in varchar2
                                     , p_data_hora         in date
                                     , p_customer_trx_id   in number
                                     , p_erro              in varchar2) as
  begin
    for r_din in(select cab.id_sequencial, pag.id_sequencial id_pagam
                   from tb_anali_ebs_ped_venda_pagam@intprd pag
                      , tb_anali_ebs_ped_venda_cab@intprd   cab
                  where cab.id_sequencial     = pag.id_ped_venda_cab
                    and cab.envio_status is null
                    and cab.tipo_ordem        = 1
                    and cab.data_hora         = p_data_hora 
                    and lpad(cab.organizacao_venda,3,0) = lpad(p_organizacao_venda,3,0)
                    and pag.tipo_pagamento in('0')) loop
      if p_customer_trx_id > 0 then

        update tb_anali_ebs_ped_venda_cab@intprd set envio_status = 10, customer_trx_id = p_customer_trx_id, envio_data_hora = sysdate
         where id_sequencial = r_din.id_sequencial;

        update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = 10
         where id_sequencial = r_din.id_pagam;

      else
        update tb_anali_ebs_ped_venda_cab@intprd set envio_status = 30, envio_erro = p_erro, envio_data_hora = sysdate
         where id_sequencial = r_din.id_sequencial;

        update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = 30, envio_erro = p_erro, envio_data_hora = sysdate
         where id_sequencial = r_din.id_pagam;

      end if;   

    end loop;  
    commit;
  end AtualizaBarramentoDinheiro;

  procedure processar_mov_equals(errbuf       out varchar2
                                ,retcode      out number
                                ,p_lote_unico in varchar2
                                ) as
    CURSOR list_errors IS
    SELECT trx_header_id
         , trx_line_id
         , trx_salescredit_id
         , trx_dist_id
         , trx_contingency_id
         , error_message
         , invalid_value
      FROM ar_trx_errors_gt;

    l_org_id              org_organization_definitions.operating_unit%type;
    l_return_status       varchar2(1);
    l_msg_count           number;
    l_msg_data            varchar2(2000);
    v_ship_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_bill_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_account_number      hz_cust_accounts.account_number%type;
    v_cust_account_id     hz_cust_accounts.cust_account_id%type;
    w_cust_trx_type_id    ra_cust_trx_types_all.cust_trx_type_id%type;
    w_sequencial          number;
    l_set_of_book_id      number;
    w_terms_id            number;
    w_batch_source_id     number;
    l_trx_line_id         number;
    l_batch_source_id     ra_batch_sources_all.batch_source_id%type;
    l_cust_acct_site_id   hz_cust_acct_sites_all.cust_acct_site_id%TYPE;
    l_ship_to_site_use_id hz_cust_site_uses_all.site_use_id%TYPE;
    l_ship_to_address_id number;
    -----
    l_trx_header_id        number;
    l_batch_source_rec     ar_invoice_api_pub.batch_source_rec_type;
    l_trx_header_tbl       ar_invoice_api_pub.trx_header_tbl_type;
    l_trx_lines_tbl        ar_invoice_api_pub.trx_line_tbl_type;
    l_trx_dist_tbl         ar_invoice_api_pub.trx_dist_tbl_type;
    l_trx_salescredits_tbl ar_invoice_api_pub.trx_salescredits_tbl_type;
    l_customer_trx_id      apps.ra_customer_trx_all.customer_trx_id%type;
    l_rErrorlist           varchar2(4000);
    w_erro                 varchar2(4000);
  begin
    l_org_id := fnd_global.org_id;
    mo_global.init('AR');
    mo_global.set_policy_context('S',l_org_id);

    fnd_file.put_line(fnd_file.log,' ');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'*********** INICIO DO PROCESSAMENTO TRANSACAO ***********');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,' ');

    for r_mov in(
                 -- trader mov
                 select x.*, (select count(distinct parcela) from xxven_ar_mov_lines where lote_unico = x.lote_unico and tipo_movimento in('A1','A2','A3','A4','A5') and header_id = x.header_id and tipo_movimento = x.tipo_movimento) qtd_parcela
                      , case x.produto
                       when '1' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'CREDITO_A_VISTA' ||'_'|| x.bandeira) 
                       when '2' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'DEBITO_A_VISTA' ||'_'|| x.bandeira) 
                       when '3' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PARCELADO_LOJA' ||'_'|| x.bandeira) 
                       when '4' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'ALIMENTACAO' ||'_'|| x.bandeira) 
                       when '5' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'REFEICAO' ||'_'|| x.bandeira) 
                       when '6' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PEDAGIO' ||'_'|| x.bandeira) 
                       when '7' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'COMBUSTIVEL' ||'_'|| x.bandeira) 
                       when '8' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'CULTURA' ||'_'|| x.bandeira) 
                       when '9' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'SAQUE' ||'_'|| x.bandeira) 
                       when '10' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PRE_DATADO' ||'_'|| x.bandeira) 
                       when '11' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PARCELADO_EMISSOR' ||'_'|| x.bandeira) 
                       when '12' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'CONVENIO' ||'_'|| x.bandeira) 
                       when '13' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'BOLETO_BANCARIO' ||'_'|| x.bandeira) 
                       when '14' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PAGAMENTO_ONLINE' ||'_'|| x.bandeira) 
                       when '15' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'RECEBIMENTO_FATURA' ||'_'|| x.bandeira) 
                       when '16' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'LINHA_CREDITO' ||'_'|| x.bandeira) 
                       when '17' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'APOSTA' ||'_'|| x.bandeira) 
                       when '18' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PREMIO' ||'_'|| x.bandeira) 
                       when '19' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'TRANSFERENCIA_ONLINE' ||'_'|| x.bandeira) 
                       when '20' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'VOUCHER' ||'_'|| x.bandeira) 
                       when '21' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PAGAMENTO_FATURA' ||'_'|| x.bandeira) 
                       when '22' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'RECARGA_CELULAR' ||'_'|| x.bandeira) 
                       when '23' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'DEBITO_AUTORIZADO' ||'_'|| x.bandeira) 
                       when '24' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'PAGAMENTO_APP' ||'_'|| x.bandeira) 
                       when '99' then (x.organizacao_venda || '_' || x.adiquirente ||'_'|| 'OUTROS' ||'_'|| x.bandeira) 
                     END ct_reference 
                   from (
                 select upper(lin.adiquirente) adiquirente, lin.organization_id, lin.lote_unico, upper(lin.bandeira) bandeira, lin.data_movimento data_hora
                      , ood.operating_unit, ood.organization_code organizacao_venda, hea.header_id
                      , msi.inventory_item_id item_id, msi.segment1, msi.global_attribute3 origem, msi.description item_descricao, msi.primary_uom_code uom_code
                      , msi.global_attribute2, msi.global_attribute4 tipo_fiscal, msi.global_attribute5 sit_federal, msi.global_attribute6 sit_estadual, lin.produto
                      , lin.tipo_movimento, sum(lin.valor_liquido) valor_item
                   from xxven_ar_mov_lines           lin
                      , xxven_ar_mov_header          hea
                      , org_organization_definitions ood
                      , mtl_system_items_b           msi
                  where lin.header_id       = hea.header_id
                    and hea.status          != 'C'
                    and NVL(lin.status,'U') != 'C'
                    and lin.organization_id is not null
                    and lin.organization_id = ood.organization_id
                    and msi.segment1        = '67247'
                    and msi.organization_id = 174
                    and lin.adiquirente in('1','2','4','12')
                    and lin.tipo_movimento in('A1','A2','A3','A4','A5')
                    and lin.lote_unico = nvl(p_lote_unico, lin.lote_unico)
                 group by lin.adiquirente, lin.organization_id, lin.lote_unico, lin.bandeira, lin.data_movimento
                        , ood.operating_unit, ood.organization_code, hea.header_id
                        , msi.inventory_item_id, msi.segment1, msi.global_attribute3, msi.description, msi.primary_uom_code
                        , msi.global_attribute2, msi.global_attribute4, msi.global_attribute5, msi.global_attribute6, lin.produto,lin.tipo_movimento
                 order by ood.organization_code) x

                      ) loop

      --dbms_output.put_line('Lote unico: ' || r_mov.lote_unico);

       FND_FILE.PUT_LINE(FND_FILE.LOG,'Passou 0');
      if valida_cliente_cartao(w_sequencial, r_mov.organizacao_venda, r_mov.operating_unit, v_ship_to, v_bill_to, v_account_number, v_cust_account_id, r_mov.adiquirente) and
         get_terms(r_mov.lote_unico, r_mov.qtd_parcela, w_terms_id, r_mov.adiquirente, l_org_id)  then

       FND_FILE.PUT_LINE(FND_FILE.LOG,'Passou 1');
       FND_FILE.PUT_LINE(FND_FILE.LOG,'w_terms_id ' || w_terms_id);
       -- dbms_output.put_line('Terms: ' || w_terms_id);

        begin
          select rct.cust_trx_type_id  into w_cust_trx_type_id
            from ra_cust_trx_types_all rct
           where rct.org_id = r_mov.operating_unit
             and name   = '5102_5405_ANALISA';
        exception
          when no_data_found then
            w_cust_trx_type_id := 0;
            fnd_file.put_line(fnd_file.log,'Não encontrado tipo dde nota fiscal.');
        end;

        begin
          select batch_source_id into w_batch_source_id
           from ra_batch_sources_all
          where org_id                            = r_mov.operating_unit
            and attribute1                        = r_mov.organizacao_venda
            and status                            = 'A'
            and batch_source_type                 = 'INV'
            and end_date                          is null
            and nvl(global_attribute5,'N')         = 'N'
            and nvl(auto_trx_numbering_flag, 'N') = 'Y'
            and rownum = 1;
        exception
          when no_data_found then
            w_batch_source_id := null;
            fnd_file.put_line(fnd_file.log,'Não encontrado a origem da organização ' || r_mov.organizacao_venda);
        end;

        SELECT ra_customer_trx_s.nextval--ra_customer_trx_lines_s.nextval --ra_customer_trx_s.nextval
          INTO l_trx_header_id
          FROM dual;

        ---- cliente --
        BEGIN
          SELECT cust_acct_site_id
            INTO l_cust_acct_site_id
            FROM hz_cust_acct_sites_all
           WHERE cust_account_id = v_cust_account_id --l_cust_account_id
             AND status          = 'A'
             AND org_id          = FND_GLOBAL.ORG_ID;

          --dbms_output.put_line('v_cust_account_id ' || v_cust_account_id);   
        EXCEPTION
          WHEN OTHERS THEN

            --dbms_output.put_line('não encontrou l_cust_acct_site_id');
            fnd_file.put_line(fnd_file.log, 'Não encontrou l_cust_acct_site_id Lote único: ' || r_mov.lote_unico);

        END;

        begin
          SELECT site_use_id
            INTO l_ship_to_site_use_id
            FROM hz_cust_site_uses_all
           WHERE cust_acct_site_id = l_cust_acct_site_id
             AND status            = 'A'
             AND site_use_code     = 'SHIP_TO'
             AND org_id            = FND_GLOBAL.ORG_ID;

          --dbms_output.put_line('l_cust_acct_site_id ' || l_cust_acct_site_id);      
        exception
          when others then
            --dbms_output.put_line('não encontrou l_ship_to_site_use_id');
            fnd_file.put_line(fnd_file.log, 'não encontrou l_ship_to_site_use_id Lote único: ' || r_mov.lote_unico);  

            l_ship_to_site_use_id := null;
        end;     

        BEGIN
          SELECT cust_acct_site_id
            INTO l_ship_to_address_id
            FROM hz_cust_site_uses_all
           WHERE site_use_id = l_ship_to_site_use_id;

           --dbms_output.put_line('l_ship_to_address_id ' || l_ship_to_address_id);      
        EXCEPTION
          WHEN OTHERS THEN
            --dbms_output.put_line('nao encontrado l_ship_to_address_id');
            --fnd_file.put_line(fnd_file.log,'nao encontrador l_ship_to_address_id' );
            fnd_file.put_line(fnd_file.log,'nao encontrador l_ship_to_address_id  Lote único: ' || r_mov.lote_unico);
            l_ship_to_address_id := null;
        END;
        FND_FILE.PUT_LINE(FND_FILE.LOG,'Passou 2');
        if (l_ship_to_address_id is not null) and (l_ship_to_site_use_id is not null) and (l_cust_acct_site_id is not null) then
            ---------------
            -- cabeçalho
            dbms_output.put_line('Cabeçalho');      
            l_set_of_book_id                                   := fnd_profile.value('GL_SET_OF_BKS_ID');
            l_trx_header_tbl(1).trx_header_id                  := l_trx_header_id;
            --l_trx_header_tbl(1).trx_number                     := (r_mov.organizacao_venda || to_char(r_mov.data_hora,'DDMMYY') || 'T2');
            l_trx_header_tbl(1).interface_header_context       := 'EQUALS'; 
            --
            l_trx_header_tbl(1).ship_to_site_use_id            := l_ship_to_site_use_id; --p_ship_to_site_use_id;
            l_trx_header_tbl(1).ship_to_address_id             := l_ship_to_address_id; --l_ship_to_address_id;
            l_trx_header_tbl(1).bill_to_customer_id            := v_cust_account_id; --p_customer_id;
            l_trx_header_tbl(1).ship_to_customer_id            := v_cust_account_id; --p_customer_id;
            l_trx_header_tbl(1).sold_to_customer_id            := v_cust_account_id; --p_customer_id;
            --
            l_trx_header_tbl(1).term_id                        := w_terms_id; --p_term_id;
            l_trx_header_tbl(1).cust_trx_type_id               := w_cust_trx_type_id; -- p_cust_trx_type_id;
            l_trx_header_tbl(1).printing_option                := 'NOT';
            l_trx_header_tbl(1).status_trx                     := 'OP';
            l_trx_header_tbl(1).trx_date                       := trunc(r_mov.data_hora); --P_TRX_DATE;
            --l_trx_header_tbl(1).GL_date                        := trunc(r_mov.data_hora); --P_GL_DATE;
            l_trx_header_tbl(1).trx_currency                   := 'BRL';
            l_trx_header_tbl(1).attribute11                    := r_mov.lote_unico;
            l_trx_header_tbl(1).global_attribute_category      := 'JL.BR.ARXTWMAI.Additional Info';
            l_trx_header_tbl(1).org_id                         := r_mov.operating_unit; --l_nOrg_id;

            ---------------------------------------
            -- Populate batch source information --
            ---------------------------------------
            l_batch_source_rec.batch_source_id                 := w_batch_source_id;
            --l_batch_source_rec.default_date                    := trunc(sysdate);
            ---------------------------------
            -- Populate line 1 information --
            ---------------------------------
            BEGIN
              SELECT ra_customer_trx_lines_s.nextval
                INTO l_trx_line_id
                FROM dual;
            END;

            dbms_output.put_line('lines');

            l_trx_lines_tbl(1).trx_header_id                   := l_trx_header_id;
            l_trx_lines_tbl(1).trx_line_id                     := l_trx_line_id;
            l_trx_lines_tbl(1).line_number                     := 1;
            l_trx_lines_tbl(1).quantity_invoiced               := 1;
            l_trx_lines_tbl(1).unit_selling_price              := r_mov.valor_item;
            l_trx_lines_tbl(1).line_type                       := 'LINE';
            l_trx_lines_tbl(1).warehouse_id                    := r_mov.organization_id;
            l_trx_lines_tbl(1).inventory_item_id               := r_mov.item_id;
            l_trx_lines_tbl(1).uom_code                        := r_mov.uom_code;
            l_trx_lines_tbl(1).global_attribute_category       := 'JL.BR.ARXTWMAI.Additional Info';
            l_trx_lines_tbl(1).unit_standard_price             := r_mov.valor_item;
            --l_trx_lines_tbl(1).memo_line_id                  := 1006; -- p_memo_line_id;
            --l_trx_lines_tbl(1).description                   := 'CARTAO DE CREDITO';

            BEGIN
              --dbms_output.put_line('passou 1');   
              AR_INVOICE_API_PUB.create_single_invoice ( p_api_version          => 1.0
                                                       , p_init_msg_list        => FND_API.G_TRUE
                                                       , p_commit               => FND_API.G_TRUE
                                                       , p_batch_source_rec     => l_batch_source_rec
                                                       , p_trx_header_tbl       => l_trx_header_tbl
                                                       , p_trx_lines_tbl        => l_trx_lines_tbl
                                                       , p_trx_dist_tbl         => l_trx_dist_tbl
                                                       , p_trx_salescredits_tbl => l_trx_salescredits_tbl
                                                       , x_customer_trx_id      => l_customer_trx_id
                                                       , x_return_status        => l_return_status
                                                       , x_msg_count            => l_msg_count
                                                       , x_msg_data             => l_msg_data);

              --dbms_output.put_line('l_customer_trx_id: ' || l_customer_trx_id);
              --dbms_output.put_line('l_return_status: '   || l_return_status);
              --dbms_output.put_line('l_msg_count: '   || l_msg_count);

              update ra_customer_trx_all set ct_reference = substr(r_mov.ct_reference,1,length(r_mov.ct_reference)) where customer_trx_id = l_customer_trx_id;
              fnd_file.put_line(fnd_file.log,'Ocorreu erro header_id = ' || l_trx_header_id);

              for r_erro in(SELECT * FROM AR_TRX_ERRORS_GT where trx_header_id = l_trx_header_id) loop
                fnd_file.put_line(fnd_file.log,'error_message  ' || r_erro.error_message );
                fnd_file.put_line(fnd_file.log,'invalid_value  ' || r_erro.invalid_value );

              end loop;

            exception
              when others then
                FND_FILE.PUT_LINE(FND_FILE.LOG,'Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
                dbms_output.put_line('Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
            end;
            FND_FILE.PUT_LINE(FND_FILE.LOG, 'w_erro: ' || l_msg_count);
            --dbms_output.put_line('Erro na CHAMADA DA API api: AR_INVOICE_API_PUB.create_single_invoice');
            --dbms_output.put_line('l_customer_trx_id: ' || l_customer_trx_id);

            if nvl(l_customer_trx_id,0) > 0 then
              update xxven_ar_mov_lines set status = 'C', customer_trx_id = l_customer_trx_id, DESC_STATUS = 'Processado com sucesso.'
               where lote_unico = r_mov.lote_unico
                 and tipo_movimento = r_mov.tipo_movimento
                 and header_id  = r_mov.header_id;

              Set_Atualiza_Parcela(r_mov.lote_unico, r_mov.header_id, r_mov.tipo_movimento ,l_customer_trx_id); 
            else
              update xxven_ar_mov_lines set status = 'E', customer_trx_id = l_customer_trx_id, DESC_STATUS = ('Ocorreu erro header_id = ' || l_trx_header_id)
               where lote_unico = r_mov.lote_unico
                 and tipo_movimento = r_mov.tipo_movimento
                 and header_id  = r_mov.header_id;
            end if;

            commit;

            l_rErrorlist := NULL;
            FOR i IN list_errors LOOP

              l_rErrorlist := substr(i.error_message,1,80)||' - '||substr(i.invalid_value,1,80);

            END LOOP;

            IF l_return_status <> FND_API.G_RET_STS_SUCCESS OR l_rErrorlist IS NOT NULL OR l_customer_trx_id IS NULL THEN
              --
              w_erro := 'Erro-Criacao de Titulo. '||l_customer_trx_id;
              --dbms_output.put_line('Passou ');
              --dbms_output.put_line('w_erro: ' || l_msg_data);
              FND_FILE.PUT_LINE(FND_FILE.LOG, 'w_erro: ' || l_msg_data);
              --
              IF l_msg_count > 0 THEN
                --
                FOR l_msg_index in 1..l_msg_count LOOP
                  --
                  l_msg_data := FND_MSG_PUB.GET(p_msg_index =>l_msg_index,p_encoded=>'F');
                  --
                  IF l_msg_data is not null THEN
                    w_erro := SUBSTR(w_erro||' '||l_msg_data,1,200);

                    --dbms_output.put_line('w_erro: ' || w_erro);
                  END IF;
                  --
                END LOOP;
                --
              ELSE
                --
                l_msg_data := FND_MSG_PUB.GET;
                --
                IF l_msg_data IS NOT NULL THEN
                  w_erro := SUBSTR(w_erro||' '||l_msg_data,1,200);

                END IF;
                --
              END IF;
              --
              FOR C_ERRO IN (SELECT trx_header_id
                                  , trx_line_id
                                  , trx_salescredit_id
                                  , trx_dist_id
                                  , trx_contingency_id
                                  , error_message
                                  , invalid_value
                               FROM ar_trx_errors_gt)
                 LOOP
                  w_erro := SUBSTR(w_erro || substr(C_ERRO.error_message,1,80)
                                         || 'Valor invalido = ' || substr(C_ERRO.invalid_value,1,80),1,200);


              END LOOP;
              --
            END IF;
            --
            COMMIT;
        else  
          update xxven_ar_mov_lines set status = 'E', desc_status = 'Nao encontrado (lhip_to_address_id, ship_to_site_use_id, cust_acct_site_id). )'
           where lote_unico = r_mov.lote_unico;
        end if;      
      end if;
    end loop;
  end processar_mov_equals;

  procedure processar_cupom(errbuf     out varchar2
                           ,retcode    out number
                           ,p_cupom    in varchar2
                           ,p_loja     in varchar2
                           ,p_pdv      in varchar2
                           ,p_data_ini in varchar2
                           ,p_data_fim in varchar2) as

    v_sequencial          number := 0;
    l_org_id              org_organization_definitions.operating_unit%type;
    v_ship_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_bill_to             hz_cust_site_uses_all.cust_acct_site_id%type;
    v_account_number      hz_cust_accounts.account_number%type;
    v_cust_account_id     hz_cust_accounts.cust_account_id%type;
    v_pbm_empresa_cliente hz_parties.party_name%type; -- ar_customers.customer_name%type;
    v_cust_trx_type_id    ra_cust_trx_types_all.cust_trx_type_id%type;
    v_batch_source_name   ra_batch_sources_all.name%type;
    v_qtd_pagamento       number;
    v_term_id            ra_terms_b.term_id%type;
    v_pagto_hibrido      ra_terms_b.term_id%type;
    v_pagto              ra_terms_b.attribute4%type;
    v_autorizacao        varchar2(50);
    v_tipo_pagamento     varchar2(10); 
    w_organizacao_venda varchar2(3) ; 
  begin

    l_org_id := fnd_global.org_id;
    mo_global.set_policy_context('S',l_org_id);

    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'**************** INICIO DO PROCESSAMENTO ****************');
    fnd_file.put_line(fnd_file.log,'=========================================================');

    for r_cupom in (select cab.id_sequencial, cab.cupom_venda, lpad(cab.organizacao_venda,3,0) organizacao_venda, cab.data_hora, cab.tipo_ordem, cab.codigo_loja_pdv, cab.caixa, cab.sequencial, cab.data_compra, cab.valor_liquido
                         , cab.pbm_empresa, cab.pbm_autorizacao
                         , org.organization_code, org.organization_id, org.operating_unit, org.set_of_books_id
                     from tb_anali_ebs_ped_venda_cab@intprd  cab
                        , org_organization_definitions         org
                    where 1=1
                      and cab.cupom_venda       = nvl(p_cupom, cab.cupom_venda)
                      and cab.codigo_loja_pdv   = nvl(p_loja, cab.codigo_loja_pdv)
                      and cab.caixa             = nvl(p_pdv, cab.caixa)
                      and org.organization_code = lpad(cab.organizacao_venda,3,0)
                      and cab.data_hora between nvl( to_date(to_date(p_data_ini ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                                            and nvl( to_date(to_date(p_data_fim ,'RRRR/MM/DD HH24:MI:SS'),'DD/MM/RRRR'),cab.data_hora)
                      --to_date('07-07-17','dd-mm-yy') and to_date('07-07-17','dd-mm-yy')
                      and exists (select 1 from tb_anali_ebs_ped_venda_pagam@intprd
                                   where cab.id_sequencial = id_ped_venda_cab 
                                     and tipo_pagamento in('302', '303', '101','102','238','305') 
                                     and envio_status is null
                            )
                      --and nvl(cab.envio_status,10) = 10
                  order by cab.organizacao_venda, cab.data_hora, cab.cupom_venda
                   ) loop

      v_qtd_pagamento := 0;

      w_organizacao_venda := null;

      if (r_cupom.data_hora >= to_date('01/05/19','dd/mm/yy')) and (r_cupom.organizacao_venda = '015') then 
        w_organizacao_venda := '515';
      elsif (r_cupom.data_hora >= to_date('01/05/19','dd/mm/yy')) and (r_cupom.organizacao_venda = '055') then 
        w_organizacao_venda := '555';
      else
        w_organizacao_venda := r_cupom.organizacao_venda;
      end if;

      if v_sequencial != r_cupom.sequencial then
        v_sequencial := r_cupom.id_sequencial;

        begin
          select count(*)
            into v_qtd_pagamento
            from tb_anali_ebs_ped_venda_pagam@intprd taepvp
           where id_ped_venda_cab =  r_cupom.id_sequencial;

           ----------dbms_output.put_line('Quantidade de pagamentos ' || v_qtd_pagamento);
           ----------fnd_file.put_line(fnd_file.log,'  Qtd Pgto: ' || v_qtd_pagamento);

         exception
          when others then
           v_qtd_pagamento := 0;
           ----------dbms_output.put_line('NÊo localizou pagamentos 0');
           fnd_file.put_line(fnd_file.log,'     Error: NÒo foi localizado pagamento para este cupom.');
        end;

        v_autorizacao := null;


        if v_qtd_pagamento != 0 then

          if v_qtd_pagamento > 1 then
          null; --  v_term_id := v_pagto_hibrido;
            ----------dbms_output.put_line('Atribuiu o hibrido');
            ----------fnd_file.put_line(fnd_file.log,'Cond Pagto: HIBRIDO');
          else

            begin
              select codigo_modalidade || '.' || -- 401
                     instituicao       || '.' || -- MASTERCARD
                     decode(numero_parcela,'0','1',numero_parcela)       --1
                   , nsu_host || '-' || autorizacao
                into v_pagto, v_autorizacao
                from tb_anali_ebs_ped_venda_pagam@intprd taepvp
               where id_ped_venda_cab = r_cupom.id_sequencial ;

              fnd_file.put_line(fnd_file.log,'v_pagto: ' || v_pagto);

              if trim(v_autorizacao) = '-' then
                v_autorizacao := null;
              end if;

              ----------dbms_output.put_line('Condi?Êo de pagamento encontrada ' || v_term_id);
              ----------fnd_file.put_line(fnd_file.log,'Cond Pagto: ' || v_pagto);
            exception
              when no_data_found then
                fnd_file.put_line(fnd_file.log,'     Error: Não foi encontrado condição de pagamento');
                v_autorizacao := null;
                fnd_file.put_line(fnd_file.log,' ');
              when others then
                fnd_file.put_line(fnd_file.log,'     Error: Não foi encontrado condição de pagamento: ' || sqlcode || ' ' || sqlerrm);
                v_autorizacao := null;
            end;
          end if;


          begin
            select tipo_pagamento into v_tipo_pagamento 
              from tb_anali_ebs_ped_venda_pagam@intprd
             where id_ped_venda_cab =  r_cupom.id_sequencial
               and tipo_pagamento in('302', '303', '101','102','238', '305')
               and rownum = 1;

            if v_tipo_pagamento = '303' then
              v_term_id := 1015;

            elsif v_tipo_pagamento in( '238','305') then -- Desenvolvimento RAPPI
              v_term_id := 1015;
              begin
                select autorizacao_pos into r_cupom.pbm_autorizacao --v_autorizacao
                  from m_pagamentos@analisa
                 where codigo_modalidade         in(238,305)
                   and lpad(codigo_filial,3,'0') = w_organizacao_venda --r_cupom.organizacao_venda
                   and estacao                  = r_cupom.caixa
                   and cupom                    = r_cupom.cupom_venda; 



                update tb_anali_ebs_ped_venda_cab@intprd set pbm_empresa = 999 , pbm_autorizacao = r_cupom.pbm_autorizacao --v_autorizacao
                 where id_sequencial = r_cupom.id_sequencial;

                update tb_anali_ebs_ped_venda_pagam@intprd set tipo_pagamento = 303
                 where id_ped_venda_cab = r_cupom.id_sequencial;

                r_cupom.pbm_empresa := 999;
                --r_cupom.pbm_autorizacao := v_autorizacao;
                commit; 
              exception
                when others then
                  fnd_file.put_line(fnd_file.log,'Nao localizado a autorizacao RAPPI');
              end;
            elsif v_tipo_pagamento = '302' then
              v_term_id := 1026; 
            elsif v_tipo_pagamento = '101' then
              v_term_id := 1027; 
            elsif v_tipo_pagamento = '102' then
              v_term_id := 4119;
            end if;
          exception
            when no_data_found then
              v_term_id := null;
          end; 

          if get_tipo_transacao(r_cupom.operating_unit, v_sequencial, v_cust_trx_type_id)                                                                                    and
             valida_cliente(v_sequencial, w_organizacao_venda, r_cupom.operating_unit, v_ship_to, v_bill_to, v_account_number, v_cust_account_id)                      and
             get_batch_source(v_sequencial, w_organizacao_venda, r_cupom.operating_unit, v_batch_source_name)                                                          then --and
             --processa_pagamento_pedido(r_cupom.id_sequencial, r_cupom.operating_unit,r_cupom.organization_id, r_cupom.organizacao_venda, r_cupom.cupom_venda, r_cupom.caixa) then


            if valida_itens(v_sequencial, r_cupom.organization_id, r_cupom.operating_unit) then
              if r_cupom.pbm_empresa is not null then
                begin
                  select hp.PARTY_NAME  into v_pbm_empresa_cliente
                    from ar_customers act
                       , hz_parties   hp
                   where act.attribute14 = r_cupom.pbm_empresa
                     and hp.customer_key = act.customer_key
                     and rownum = 1;

                  fnd_file.put_line(fnd_file.log,'PBM Empresa: ' || v_pbm_empresa_cliente);   
                exception
                  when others then
                  v_pbm_empresa_cliente := null;
                end;
              else
                v_pbm_empresa_cliente := null;
              end if;

              if
                processar_cupom_ar(r_cupom.id_sequencial
                                 , r_cupom.pbm_autorizacao
                                 , w_organizacao_venda --r_cupom.organizacao_venda
                                 , r_cupom.operating_unit
                                 , r_cupom.organization_id
                                 , r_cupom.set_of_books_id
                                 , v_pbm_empresa_cliente
                                 , v_cust_trx_type_id
                                 , v_ship_to
                                 , v_bill_to
                                 , v_account_number
                                 , v_cust_account_id
                                 , v_batch_source_name
                                 , v_term_id
                                 , v_autorizacao
                                 )                            then


                update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '40'
                                                             , envio_data_hora = sysdate
                 where id_sequencial = r_cupom.id_sequencial
                   and envio_status is null;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '40'
                                                             , envio_data_hora = sysdate
                 where id_ped_venda_cab = r_cupom.id_sequencial
                   and envio_status is null;

                --begin
                  for r_pag in(select id_sequencial
                                 from tb_anali_ebs_ped_venda_pagam@intprd
                                where id_ped_venda_cab = r_cupom.id_sequencial
                                  and tipo_pagamento in('302', '303', '101','102') )loop

                    update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = '40'
                                                                  , envio_data_hora = sysdate
                     where id_sequencial = r_pag.id_sequencial;

                  end loop;

                --end;  

                --update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = '40'
                --                                             , envio_data_hora = sysdate
                -- where  envio_status is null
                --   and id_ped_venda_cab = r_cupom.id_sequencial;

                --processar_movimentacao_estoque(r_cupom.id_sequencial);

                commit;

                ----------fnd_file.put_line(fnd_file.log,'Cupom processado com sucesso.');
              end if;

            end if;
          else

            retcode := '1';
          end if;
        else
           fnd_file.put_line(fnd_file.log,'Erro: Nao foi localizado pagamento para este cupom. - id_sequencial:'||r_cupom.id_sequencial);
           update tb_anali_ebs_ped_venda_cab@intprd set envio_status = '30', envio_data_hora = sysdate, envio_erro = 'NÒo foi localizado pagamento para este cupom' where id_sequencial = r_cupom.id_sequencial;
           update tb_anali_ebs_ped_venda_lin@intprd set envio_status = '30', envio_data_hora = sysdate, envio_erro = 'NÒo foi localizado pagamento para este cupom' where id_ped_venda_cab = r_cupom.id_sequencial;
        end if;
       end if;

       ----------fnd_file.put_line(fnd_file.log,' ');
    end loop;

    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '33051000'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 25328 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '33051000'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 39152 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '33074900'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA'    , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '00' where INVENTORY_ITEM_ID = 39153 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '22072019'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 39154 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '22072019'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 39155 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '33041000'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 39156 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '30049099X2' , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 38156 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '21050010'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '0' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '00' where INVENTORY_ITEM_ID = 38151 ;
    update apps.ra_interface_lines_all  set LINE_GDF_ATTRIBUTE2 = '21050010'   , LINE_GDF_ATTRIBUTE3 = 'REVENDA_ST' , LINE_GDF_ATTRIBUTE4 = '5' , LINE_GDF_ATTRIBUTE5 = '00' , LINE_GDF_ATTRIBUTE6 = '99' , LINE_GDF_ATTRIBUTE7 = '60' where INVENTORY_ITEM_ID = 42140 ;

    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'***************** FIM DO PROCESSAMENTO ******************');
    fnd_file.put_line(fnd_file.log,'=========================================================');

exception when others then retcode := '2'; fnd_file.put_line(fnd_file.log,'ERRO NO PROCESSAMENTO DO CUPOM - ID_SEQUENCIAL:'||v_sequencial); fnd_file.put_line(fnd_file.log,'ERRO:'||SQLERRM);
end processar_cupom;

  function processa_pagamento_pedido(p_sequencial        in number
                                   , p_operating_unit    in number
                                   , p_organization_id   in number
                                   , p_organizacao_venda in varchar2
                                   , p_cupom             in varchar2
                                   , p_caixa             in varchar2) return boolean as
    l_term_id number;
    l_pedido varchar2(50);
    l_error number := 0;
  begin
    l_pedido := '1' || p_organizacao_venda || lpad(p_caixa,3,'0') || lpad(p_cupom,13,'0');

    --FND_FILE.PUT_LINE(FND_FILE.LOG,'Cupom sendo processado: ' || l_pedido);
    ----------dbms_output.put_line('Cupom sendo processado: ' || l_pedido);
    for r_pagam in (select distinct
                             taepvp.id_sequencial,
                             taepvp.id_ped_venda_cab,
                             taepvp.cupom_venda,
                             taepvp.pdv_serie,
                             taepvp.loja_tipo,
                             taepvp.organizacao_venda,
                             taepvp.numero_ordem,
                             taepvp.data_hora,
                             taepvp.tipo_pagamento,
                             taepvp.valor_pago,
                             taepvp.codigo_modalidade,
                             taepvp.banco,
                             taepvp.cheque,
                             taepvp.codigo_convenio,
                             taepvp.codigo_filial_convenio,
                             taepvp.agencia,
                             taepvp.conta_corrente,
                             taepvp.numero_devolucao,
                             taepvp.origem_pagamento,
                             taepvp.codigo_rede,
                             taepvp.autorizacao,
                             taepvp.nsu_sitef,
                             taepvp.codigo_transacao,
                             taepvp.numero_documento,
                             taepvp.numero_doc_cancelado,
                             decode(taepvp.numero_parcela,'0','1',taepvp.numero_parcela) numero_parcela,
                             taepvp.valor_operacao,
                             taepvp.instituicao,
                             taepvp.nsu_host
                        from tb_anali_ebs_ped_venda_pagam@intprd taepvp
                       where 1 = 1 --TAEPVP.ENVIO_STATUS IS NULL
                         and id_ped_venda_cab = p_sequencial
                         --AND CUPOM_VENDA = NVL(PC_CUPOM,CUPOM_VENDA)
                         --AND PDV_SERIE = NVL(PC_CAIXA,PDV_SERIE)
                         --AND ORGANIZACAO_VENDA = NVL(PC_LOJA,ORGANIZACAO_VENDA)
                   ) loop
      begin
        l_term_id := 0;

        select b.term_id
          into l_term_id
          from ra_terms_b b
          join ra_terms_tl tl on b.term_id = tl.term_id
           and b.zd_edition_name = tl.zd_edition_name
         where tl.language = 'PTB'
           and b.attribute4 = r_pagam.codigo_modalidade || '.' || -- 401
                              r_pagam.instituicao       || '.' || -- MASTERCARD
                              r_pagam.numero_parcela 
           and rownum = 1;

      exception
        when no_data_found then
          update tb_anali_ebs_ped_venda_pagam@intprd set envio_status    = '30'
                                                       , envio_data_hora = sysdate
                                                       , envio_erro = 'NAO ENCONTRADO INFORMACOES DE PAGAMENTO: ' ||
                                                                                r_pagam.codigo_modalidade || '.' || -- 401
                                                                                r_pagam.instituicao       || '.' || -- MASTERCARD
                                                                                r_pagam.numero_parcela
           where id_ped_venda_cab = p_sequencial
             and envio_status is null;

           update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                       , envio_data_hora = sysdate
                                                       , envio_erro = 'NAO ENCONTRADO INFORMACOES DE PAGAMENTO: ' ||
                                                                                r_pagam.codigo_modalidade || '.' || -- 401
                                                                                r_pagam.instituicao       || '.' || -- MASTERCARD
                                                                                r_pagam.numero_parcela
            where id_sequencial = p_sequencial
              and envio_status is null;

           update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                       , envio_data_hora = sysdate
            where id_ped_venda_cab = p_sequencial
              and envio_status is null;

        l_error := l_error + 1;
        fnd_file.put_line(fnd_file.log,'     Error: Não encontrado informações de pagamento: ' ||
                                                         r_pagam.codigo_modalidade || '.' || -- 401
                                                         r_pagam.instituicao       || '.' || -- MASTERCARD
                                                         r_pagam.numero_parcela);

        ----------dbms_output.put_line('NAO ENCONTRADO INFORMACOES DE PAGAMENTO: ' ||
        ----------                                                 r_pagam.codigo_modalidade || '.' || -- 401
        ----------                                                 r_pagam.instituicao       || '.' || -- MASTERCARD
        ----------                                                 r_pagam.numero_parcela);

        return false;
      when others then
        ----------dbms_output.put_line('Erro ao tentar localizar informa?ºes de pagamento');
        fnd_file.put_line(fnd_file.log,'     Error: NÊo encontrado informações de pagamento');
      end;

      l_term_id := nvl(l_term_id,0);

      if l_term_id != 0 then
        begin
            ----------dbms_output.put_line('Inserindo XXVEN_OM_FORMAS_PAGTO');
            insert into xxven.xxven_om_formas_pagto
                  (order_source_id,
                   orig_sys_document_ref,
                   pdv_serie,
                   loja_tipo,
                   org_id,
                   order_number,
                   ordered_date,
                   tipo_de_pagamento,
                   valor_pago,
                   codigo_modalidade,
                   banco,
                   cheque,
                   codigo_convenio,
                   codigo_filial_convenio,
                   agencia,
                   conta_corrente,
                   numero_devolucao,
                   origem_pagamento,
                   codigo_rede,
                   nsu_sitef,
                   codigo_transacao,
                   numero_documento,
                   numero_doc_cancelado,
                   numero_parcela,
                   valor_operacao,
                   instituicao,
                   nsu_host,
                   autorizacao,
                   term_id)
                values
                  (1001,                              --ORDER_SOURCE_ID
                   l_pedido,                                       --ORIG_SYS_DOCUMENT_REF
                   r_pagam.pdv_serie,                      --PDV_SERIE
                   r_pagam.loja_tipo,                      --LOJA_TIPO
                   p_organization_id,                              --ORG_ID
                   r_pagam.numero_ordem,                   --ORDER_NUMBER
                   r_pagam.data_hora,                      --ORDERED_DATE
                   r_pagam.tipo_pagamento,                 --TIPO_DE_PAGAMENTO
                   r_pagam.valor_pago,                     --VALOR_PAGO
                   r_pagam.codigo_modalidade,              --CODIGO_MODALIDADE
                   r_pagam.banco,                          --BANCO
                   r_pagam.cheque,                         --CHEQUE
                   r_pagam.codigo_convenio,                --CODIGO_CONVENIO
                   r_pagam.codigo_filial_convenio,         --CODIGO_FILIAL_CONVENIO
                   r_pagam.agencia,                        --AGENCIA
                   r_pagam.conta_corrente,                 --CONTA_CORRENTE
                   r_pagam.numero_devolucao,               --NUMERO_DEVOLUCAO
                   r_pagam.origem_pagamento,               --ORIGEM_PAGAMENTO
                   r_pagam.codigo_rede,                    --CODIGO_REDE
                   r_pagam.nsu_sitef,                      --NSU_SITEF
                   r_pagam.codigo_transacao,               --CODIGO_TRANSACAO
                   r_pagam.numero_documento,               --NUMERO_DOCUMENTO
                   r_pagam.numero_doc_cancelado,           --NUMERO_DOC_CANCELADO
                   r_pagam.numero_parcela,                 --NUMERO_PARCELA
                   r_pagam.valor_operacao,                 --VALOR_OPERACAO
                   r_pagam.instituicao,                    --INSTITUICAO
                   r_pagam.nsu_host,                       --NSU_HOST
                   r_pagam.autorizacao,                    --AUTORIZACAO
                   l_term_id                                       --TERM_ID
                   );
          --commit;
        exception
          when others then
            fnd_file.put_line(fnd_file.log,'Erro ao inserir tipo pagamento: ' || sqlcode || ' ' || sqlerrm);
            ----------dbms_output.put_line('Erro ao inserir tipo pagamento: ' || sqlcode || ' ' || sqlerrm);

        end;
      end if;
    end loop;

    if l_error != 0 then
      return false;
    else
      return true;
    end if;

  end processa_pagamento_pedido;

  function valida_cliente_cartao(p_sequencial        in  number
                               , p_organizacao_venda in  varchar2
                               , p_operating_unit    in  org_organization_definitions.operating_unit%type
                               , p_ship_to           out hz_cust_site_uses_all.site_use_id%type
                               , p_bill_to           out hz_cust_site_uses_all.site_use_id%type
                               , p_account_number    out hz_cust_accounts.account_number%type
                               , p_cust_account_id   out hz_cust_accounts.cust_account_id%type
                               , p_adiquirente       in varchar2 ) return boolean as
    w_adiquirente varchar2(150);
  begin
    --fnd_file.put_line(fnd_file.log,'Validando cliente...');
    ----------dbms_output.put_line('Validando cliente...');


    begin
      dbms_output.put_line('Adiquirente: ' || p_adiquirente);
      if p_adiquirente = '1' then
        w_adiquirente := 'CIELO SA';
      elsif p_adiquirente = '2' then
        w_adiquirente := 'REDECARD SA';
      elsif p_adiquirente = '4' then
        w_adiquirente := 'TEMPO SERVICOS LTDA'; 
      elsif p_adiquirente = '12' then  
        w_adiquirente := 'SODEXO PASS DO BRASIL SERVICOS E COMERCIO S.A.'; 
      end if;

      FND_FILE.PUT_LINE(FND_FILE.LOG,'Adiquirente ' || w_adiquirente);

      select hcu.cust_acct_site_id bill_to , hca.cust_account_id orig_system_bill_customer_id
           , (select  hcu2.cust_acct_site_id
                from hz_cust_site_uses_all hcu2
               where hcu2.cust_acct_site_id = hcs.cust_acct_site_id
                 and hcu2.site_use_code = 'SHIP_TO'
              ) ship_to --, hp.party_name
        into p_bill_to, p_cust_account_id, p_ship_to --, p_account_number
        from hz_parties             hp
           , hz_cust_accounts       hca
                         , hz_party_sites         hps
                         , hz_cust_acct_sites_all hcs
                         , hz_cust_site_uses_all  hcu
                         , hz_locations           hl
                    where hp.party_name         =  w_adiquirente --hp.attribute2         = lpad(p_organizacao_venda,3,'0')--'015'
                      and hca.party_id          = hp.party_id
                      and hp.party_id           = hps.party_id
                      and hcs.cust_account_id   = hca.cust_account_id
                      and hcs.party_site_id     = hps.party_site_id
                      and hcs.cust_acct_site_id = hcu.cust_acct_site_id
                      and hl.location_id        = hps.location_id
                      and hcs.status = 'A'
                      AND HCU.primary_flag      = 'Y'
                      and hcs.org_id = p_operating_unit
                      and hcu.site_use_code = 'BILL_TO';

      ----------dbms_output.put_line('p_ship_to: ' || p_ship_to || ' p_bill_to: ' || p_bill_to || ' p_account_number: ' || p_account_number || ' p_cust_account_id: ' || p_cust_account_id);
      ----------fnd_file.put_line(fnd_file.log,'   Cliente: ' || lpad(p_organizacao_venda,3,'0'));

      return true;
    exception
      when no_data_found then

        fnd_file.put_line(fnd_file.log,'   Cliente não localizado: ' || p_adiquirente);
        --dbms_output.put_line('   Cliente não localizado: ' || p_adiquirente);
        return false;
      when others then
        fnd_file.put_line(fnd_file.log,'Erro ' || sqlerrm);
        return false;
    end;
  end valida_cliente_cartao;

  function valida_cliente(p_sequencial        in  number
                        , p_organizacao_venda in  varchar2
                        , p_operating_unit    in  org_organization_definitions.operating_unit%type
                        , p_ship_to           out hz_cust_site_uses_all.site_use_id%type
                        , p_bill_to           out hz_cust_site_uses_all.site_use_id%type
                        , p_account_number    out hz_cust_accounts.account_number%type
                        , p_cust_account_id   out hz_cust_accounts.cust_account_id%type
                        , tipo                in varchar2 default 'N') return boolean as

  begin
    fnd_file.put_line(fnd_file.log,'Validando cliente...');
    ----------dbms_output.put_line('Validando cliente...');

    begin
      fnd_file.put_line(fnd_file.log,'Validando cliente...2');
      fnd_file.put_line(fnd_file.log,'   Cliente: ' || lpad(p_organizacao_venda,3,'0'));
      select hcu.cust_acct_site_id bill_to , hca.cust_account_id orig_system_bill_customer_id
           , (select  hcu2.cust_acct_site_id
                from hz_cust_site_uses_all hcu2
               where hcu2.cust_acct_site_id = hcs.cust_acct_site_id
                 and hcu2.site_use_code = 'SHIP_TO'
              ) ship_to --, hp.party_name
        into p_bill_to, p_cust_account_id, p_ship_to --, p_account_number
        from hz_parties             hp
           , hz_cust_accounts       hca
                         , hz_party_sites         hps
                         , hz_cust_acct_sites_all hcs
                         , hz_cust_site_uses_all  hcu
                         , hz_locations           hl
                    where hp.attribute2         = lpad(p_organizacao_venda,3,'0')--'015'
                      and hca.party_id          = hp.party_id
                      and hp.party_id           = hps.party_id
                      and hcs.cust_account_id   = hca.cust_account_id
                      and hcs.party_site_id     = hps.party_site_id
                      and hcs.cust_acct_site_id = hcu.cust_acct_site_id
                      and hl.location_id        = hps.location_id
                      and hcs.status = 'A'
                      and hcs.org_id = p_operating_unit
                      and hcu.site_use_code = 'BILL_TO';

      ----------dbms_output.put_line('p_ship_to: ' || p_ship_to || ' p_bill_to: ' || p_bill_to || ' p_account_number: ' || p_account_number || ' p_cust_account_id: ' || p_cust_account_id);
      fnd_file.put_line(fnd_file.log,'   Cliente: ' || lpad(p_organizacao_venda,3,'0'));

      return true;
    exception
      when no_data_found then
        --dbms_output.put_line('Deu merda no cliente: ' || p_organizacao_venda);
        fnd_file.put_line(fnd_file.log,'Deu merda no cliente: ' || p_organizacao_venda);

        if p_sequencial > 0 then
          update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                     , envio_data_hora = sysdate
                                                     , envio_erro      = 'Não localizado o cliente para esta organiza?Êo (BILL_TO, SHIP_TO)'
           where id_sequencial = p_sequencial;

          update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                     , envio_data_hora = sysdate
                                                     , envio_erro      = 'Não localizado o cliente para esta organiza?Êo (BILL_TO, SHIP_TO)'
            where id_ped_venda_cab = p_sequencial;
        end if;

        return false;

        fnd_file.put_line(fnd_file.log,'     Error: Cliente não encontrado da organização ' || lpad(p_organizacao_venda,3,'0'));
      when others then
        fnd_file.put_line(fnd_file.log,'Validando cliente...3');
        if p_sequencial > 0 then
          fnd_file.put_line(fnd_file.log,'Não localizado o cliente para esta organização (BILL_TO, SHIP_TO) others');

          update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                     , envio_data_hora = sysdate
                                                     , envio_erro      = 'Não localizado o cliente para esta organização (BILL_TO, SHIP_TO)'
          where id_sequencial = p_sequencial;

          update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                     , envio_data_hora = sysdate
                                                     , envio_erro      = 'Não localizado o cliente para esta organização (BILL_TO, SHIP_TO)'
            where id_ped_venda_cab = p_sequencial;

        end if;

        return false;

        ----------dbms_output.put_line('NÊo localizou cliente.');
        fnd_file.put_line(fnd_file.log,'     Error: Cliente não encontrado da organização ' || lpad(p_organizacao_venda,3,'0'));

        ----------dbms_output.put_line('Outro erro cliente.'  || sqlcode || ' ' || sqlerrm);
        fnd_file.put_line(fnd_file.log,'     Error: Cliente não encontrado ' || sqlcode || ' ' || sqlerrm);
    end;
  end valida_cliente;

  function valida_itens(p_sequencial      in number
                      , p_organization_id in number
                      , p_operating_unit in number) return boolean as

    v_id              number;
    v_qtd_n_item      number;
    v_qtd             number;
    v_id_item         number;
    v_error           boolean;
    v_set_of_books_id org_organization_definitions.set_of_books_id%type;
    v_tax_code        varchar2(50);
    v_item_code       varchar2(100);
  begin
    begin
      v_error := false;
      v_item_code := NULL;
      ----------fnd_file.put_line(fnd_file.log,'Validando itens.');
      ----------dbms_output.put_line('Validando item...');
      begin
        select count(codigo_item)
          into v_qtd
          from tb_anali_ebs_ped_venda_lin@intprd
         where id_ped_venda_cab = p_sequencial;

      exception
        when others then
          v_qtd := 0;
      end;


      for r_item in (select codigo_item, id_sequencial, pis_aliquota, cofins_aliquota, icms_aliquota
                       from tb_anali_ebs_ped_venda_lin@intprd
                      where id_ped_venda_cab = p_sequencial) loop
        begin
         -- v_error := false;

          select segment1, set_of_books_id into v_id_item, v_set_of_books_id
            from mtl_system_items_b msi
               , org_organization_definitions oog
           where oog.organization_id = msi.organization_id
             and msi.organization_id = p_organization_id
             and msi.segment1        = r_item.codigo_item;

          ----------fnd_file.put_line(fnd_file.log,'Item validado: ' || r_item.codigo_item);
          ----------dbms_output.put_line('Item validado: ' || r_item.codigo_item);
          v_item_code := r_item.codigo_item;
        exception
          when no_data_found then
            v_error := true;
            update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                         , envio_erro      = 'Item nÊo est? cadastrado ou atribu¦do nesta organiza?Êo.'
                                                         , envio_data_hora = sysdate
            where id_sequencial = r_item.id_sequencial;

            fnd_file.put_line(fnd_file.log,'     Error: Item nÊo localizado ' || r_item.codigo_item);

            ----------dbms_output.put_line('Item nÊo localizado. ' || r_item.codigo_item);
          when others then
            fnd_file.put_line(fnd_file.log,'     Error: Item nÊo localizado. ' || sqlcode || ' ' || sqlerrm);
            ----------dbms_output.put_line('Erro ao localizar o item. ' || sqlcode || ' ' || sqlerrm);
        end;

        if not v_error then
        -- validacaao aliquota imposto
          if nvl(r_item.pis_aliquota, 0) > 0 then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where tax = 'PIS_C'
                 and tax_rate          = r_item.pis_aliquota
                 and set_of_books_id   = v_set_of_books_id
                 and org_id            = p_operating_unit
                 and global_attribute6 = '01'
                 and end_date          is null;
            exception
              when no_data_found then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota) ;
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;

              when others then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota PIS: ' || r_item.pis_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;
            end;
          end if;

          if nvl(r_item.cofins_aliquota, 0) > 0 then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where tax = 'COFINS_C'
                 and tax_rate        = r_item.cofins_aliquota
                 and set_of_books_id = v_set_of_books_id
                 and global_attribute7 = '01'
                 and org_id          = p_operating_unit
                 and end_date          is null;
            exception
              when no_data_found then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota) ;
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;

              when others then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota COFINS: ' || r_item.cofins_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;
            end;
          end if;

          if nvl(r_item.icms_aliquota, 0) > 0 then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where tax = 'ICMS_C'
                 and tax_rate        = r_item.icms_aliquota
                 and set_of_books_id = v_set_of_books_id
                 and org_id          = p_operating_unit
                 and end_date          is null;
            exception
              when no_data_found then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota) ;
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;

              when others then
                fnd_file.put_line(fnd_file.log,'Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                ----------dbms_output.put_line('Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota || ' ' || sqlcode || ' ' || sqlerrm);
                v_error := true;

                update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                             , envio_erro      = 'Erro ao tentar encontrar Aliquota ICMS: ' || r_item.icms_aliquota
                                                             , envio_data_hora = sysdate
                where id_sequencial = r_item.id_sequencial;
            end;
          end if;

        end if;
      end loop;

      if v_error then

        update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                   , envio_data_hora = sysdate
        where id_sequencial = p_sequencial;

        update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                   , envio_data_hora = sysdate
         where id_ped_venda_cab = p_sequencial
           and envio_status is null;

        update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = '30'
                                                     , envio_data_hora = sysdate
         where  envio_status is null
           and id_ped_venda_cab = p_sequencial;

        commit;

        return false;
      elsif v_qtd = 0 THEN
        update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                   , envio_data_hora = sysdate
                                                   , envio_erro      = 'Não existem linhas para este cupom'
        where id_sequencial = p_sequencial;


        update tb_anali_ebs_ped_venda_pagam@intprd set envio_status = '30'
                                                     , envio_data_hora = sysdate
                                                     , envio_erro      = 'Não existem linhas para este cupom'
         where  envio_status is null
           and id_ped_venda_cab = p_sequencial;

        commit;

        return false;
      else
        return true;
      end if;

    exception
      when others then
        fnd_file.put_line(fnd_file.log,'Erro ao tentar localizar item ' || v_item_code || sqlcode || ' ' || sqlerrm); -- erro inesperado
        ----------dbms_output.put_line('Erro ao tentar localizar item. ' || sqlcode || ' ' || sqlerrm);
    end;

  end valida_itens;

  function get_tipo_transacao(p_operating_unit   in number
                            , p_sequencial       in number
                            , p_cust_trx_type_id out number)return boolean as

    v_organization_code org_organization_definitions.organization_code%type;
  begin
    --dbms_output.put_line('Inicio Tipo de transa?Êo encontrada. ' ||  '5102_5405_ANALISA');
    begin
      select rct.cust_trx_type_id  into p_cust_trx_type_id
        from ra_cust_trx_types_all rct
       where rct.org_id = p_operating_unit
         and name   = '5102_5405_ANALISA';

      ----------fnd_file.put_line(fnd_file.log,'Tipo de transa?Êo encontrada. ' ||  '5102_5405_ANALISA');
      ----------dbms_output.put_line('Tipo de transa?Êo encontrada. ' ||  '5102_5405_ANALISA');
      return true;
    exception
      when no_data_found then
        p_cust_trx_type_id := null;

        update tb_anali_ebs_ped_venda_cab@intprd set envio_status    = '30'
                                                   , envio_data_hora = sysdate
                                                   , envio_erro      = 'NÊo localicado o tipo de transação 5102_5405_ANALISA para esta organização ' || v_organization_code
         where id_sequencial = p_sequencial;

        update tb_anali_ebs_ped_venda_lin@intprd set envio_status    = '30'
                                                   , envio_data_hora = sysdate
                                                   , envio_erro      = 'Não localicado o tipo de transação 5102_5405_ANALISA para esta organização ' || v_organization_code
          where id_ped_venda_cab = p_sequencial;

      fnd_file.put_line(fnd_file.log,'     Error: Tipo de transação não encontrada. ' ||  '5102_5405_ANALISA');
      ----------dbms_output.put_line('Tipo de transa?Êo nÊo encontrada. ' ||  '5102_5405_ANALISA');

      when others then
        fnd_file.put_line(fnd_file.log,'     Error: Tipo de transação não encontrada. ' || sqlcode || ' ' || sqlerrm);
        ----------dbms_output.put_line('Erro ao tentar encontrar tipo de transa?Êo . ' || sqlcode || ' ' || sqlerrm);
       return false;
    end;


  end ;

  function get_batch_source(p_sequencial        in number
                          , p_organizacao_venda in varchar2
                          , p_operating_unit    in number
                          , p_batch_source_name out varchar2) return boolean as
  begin
    begin
      fnd_file.put_line(fnd_file.log,'p_organizacao_venda: ' || p_organizacao_venda || ' p_operating_unit: ' || p_operating_unit);
      select  name into p_batch_source_name
        from ra_batch_sources_all
       where org_id                            = p_operating_unit
         and attribute1                        = p_organizacao_venda
         and batch_source_type                 = 'FOREIGN'
         and nvl(auto_trx_numbering_flag, 'N') = 'N';

      ----------dbms_output.put_line('Batch sourse name: ' || p_batch_source_name);
      ----------fnd_file.put_line(fnd_file.log,'    Origem: ' || p_batch_source_name);
      return true;
    exception
      when others then
        p_batch_source_name := null;
        --v_tipo_id   := null;
        --dbms_output.put_line('Batch sourse name nÊo encontrado');
        fnd_file.put_line(fnd_file.log,'     Error: Origem não encontrada ');
        return false;
    end;
  end get_batch_source;

  function  processar_cupom_ar(p_sequencial          in  number
                             , p_pbm_autorizacao     in  varchar2
                             , p_organizacao_venda   in  varchar2
                             , p_operating_unit      in  org_organization_definitions.operating_unit%type
                             , p_organization_id     in  org_organization_definitions.organization_id%type
                             , p_set_of_books_id     in  org_organization_definitions.set_of_books_id%type
                             , p_pbm_empresa_cliente in  ar_customers.customer_name%type
                             , p_cust_trx_type_id    in  ra_cust_trx_types_all.cust_trx_type_id%type
                             , p_ship_to             in  hz_cust_site_uses_all.cust_acct_site_id%type
                             , p_bill_to             in  hz_cust_site_uses_all.cust_acct_site_id%type
                             , p_account_number      in  hz_cust_accounts.account_number%type
                             , p_cust_account_id     in  hz_cust_accounts.cust_account_id%type
                             , p_batch_source_name   in  ra_batch_sources_all.name%type
                             , p_term_id             in  ra_terms.term_id%type
                             , p_autoricazao         in  varchar2) return boolean as

    v_ident_cupom          varchar(50);
    v_stock_enabled_flag   VARCHAR2(1);
    l_error number         := 0;
    v_tax_code             ra_interface_lines_all.tax_code%type;
    v_distribution_id      gl_code_combinations.code_combination_id%type;
    v_empresa              gl_code_combinations.segment1%type;
    v_filial               gl_code_combinations.segment2%type;
    v_centro_custo         gl_code_combinations.segment3%type;
    v_canal                gl_code_combinations.segment5%type;
    v_chart_of_accounts_id org_organization_definitions.chart_of_accounts_id%type;
    v_trx_number           varchar2(30);
    v_term_id             number;
  begin

    ----------fnd_file.put_line(fnd_file.log,'Processando cupom...' );
    ----------dbms_output.put_line('Processando cupom...');

    begin
      select gl.segment1
           , gl.segment2
           , (select chart_of_accounts_id from org_organization_definitions where organization_id = p_organization_id)
           , gl.segment3
           , gl.segment5
        into v_empresa
           , v_filial
           , v_chart_of_accounts_id
           , v_centro_custo
           , v_canal
        from mtl_parameters mp
           , gl_code_combinations gl
       where mp.cost_of_sales_account = gl.code_combination_id
         and mp.organization_id       = p_organization_id;


      select code_combination_id into v_distribution_id
          from gl_code_combinations
         where chart_of_accounts_id = v_chart_of_accounts_id
           and segment1             = v_empresa
           and segment2             = v_filial
           and segment3             = v_centro_custo
           and segment4             = '114101998'
           and segment5             = v_canal
           and segment6             = '0000000'
           and segment7             = '00'
           and segment8             = '000000'
           and segment9             = '000000';

      ----------dbms_output.put_line('Distribution_id ' || v_distribution_id);
    exception
      when others then
        v_distribution_id := null;
        ----------dbms_output.put_line('NÊo coseguiu localizar a distribion.. ');
        ----------dbms_output.put_line('Empresa: ' || v_empresa);
        ----------dbms_output.put_line('Filial: '  || v_filial);
        ----------dbms_output.put_line('Custo: '   || v_centro_custo);
        ----------dbms_output.put_line('Conta: 114101999');
        ----------dbms_output.put_line('Chart_of_accounts_id' || v_chart_of_accounts_id);

    end;

    for r_item in (select msi.inventory_item_id item_id, msi.segment1, msi.global_attribute3 origem, msi.description item_descricao, msi.primary_uom_code uom_code
                        , msi.global_attribute2, msi.global_attribute4 tipo_fiscal, msi.global_attribute5 sit_federal, msi.global_attribute6 sit_estadual
                        , pag.valor_pago valor_item, cab.cupom_venda, cab.organizacao_venda, cab.data_hora, cab.caixa estacao, cab.id_sequencial sequencial, pag.tipo_pagamento
                        , (select mc.segment1
                             from mtl_item_categories mic
                                , mtl_categories mc
                                , mtl_category_sets mcs
                            where mic.inventory_item_id = msi.inventory_item_id
                              and mic.organization_id   = msi.organization_id
                              and mic.category_id       = mc.category_id
                              and mcs.structure_id      = mc.structure_id
                              and mcs.category_set_name = 'FISCAL_CLASSIFICATION'
                           )ncm
                     from mtl_system_items_b msi
                        , tb_anali_ebs_ped_venda_pagam@intprd pag
                        , tb_anali_ebs_ped_venda_cab@intprd cab
                    where msi.segment1 = '67247'
                      and msi.organization_id = p_organization_id --174
                      and pag.id_ped_venda_cab = p_sequencial --20180427048035222620180428
                      and cab.id_sequencial = pag.id_ped_venda_cab
                      and pag.tipo_pagamento in('302', '303', '101','102')
                  ) loop


        if r_item.tipo_pagamento    = '303' then
          v_term_id := 1015;
        elsif r_item.tipo_pagamento = '302' then
          v_term_id := 1026; 
        elsif r_item.tipo_pagamento = '101' then
          v_term_id := 1027; 
        elsif r_item.tipo_pagamento = '102' then
          v_term_id := 4119;
        end if;


        if r_item.tipo_pagamento in('302', '101','102') then
          v_ident_cupom := (r_item.tipo_pagamento || r_item.organizacao_venda || lpad(r_item.estacao,3,'0') || lpad(r_item.cupom_venda,13,'0'));

          v_trx_number := (r_item.cupom_venda || '-'|| r_item.estacao || '/' || r_item.tipo_pagamento);
        else
          v_ident_cupom := '1' || r_item.organizacao_venda || lpad(r_item.estacao,3,'0') || lpad(r_item.cupom_venda,13,'0');

          v_trx_number := r_item.cupom_venda || '-'|| r_item.estacao ;
        end if;  
        ----------dbms_output.put_line('Item: ' || r_item.codigo_item);
        -- insere linha normal
        begin
          insert into ar.ra_interface_lines_all(interface_line_context
                                           , interface_line_attribute1
                                           , interface_line_attribute2
                                           , interface_line_attribute3
                                           , interface_line_attribute4
                                           , batch_source_name
                                           , set_of_books_id
                                           , line_type
                                           , description
                                           , currency_code
                                           , amount
                                           , cust_trx_type_id
                                           , gl_date
                                           , term_id
                                           , orig_system_bill_customer_id
                                           , orig_system_bill_address_id
                                           , orig_system_ship_customer_id
                                           , orig_system_ship_address_id
                                           , orig_system_sold_customer_id
                                           , conversion_type
                                           , conversion_rate
                                           , trx_number
                                           , quantity
                                           , unit_selling_price
                                           , inventory_item_id
                                           , header_attribute4
                                           , header_attribute6
                                           , header_attribute7
                                           , header_attribute10
                                           , header_attribute11
                                           , uom_code
                                           , created_by
                                           , creation_date
                                           , last_updated_by
                                           , last_update_date
                                           , last_update_login
                                           , org_id
                                           , amount_includes_tax_flag
                                           , header_gdf_attr_category  --JL.BR.ARXTWMAI.Additional Info
                                           , line_gdf_attr_category
                                           , line_gdf_attribute2
                                           , line_gdf_attribute3
                                           , line_gdf_attribute4
                                           , line_gdf_attribute5
                                           , line_gdf_attribute6
                                           , line_gdf_attribute7
                                           , warehouse_id
                                           , ship_date_actual
                                           , attribute9
                                            ) values (
                                             'ANALISA'                           --INTERFACE_LINE_CONTEXT     Sistema Legado
                                           , v_ident_cupom                       --INTERFACE_LINE_ATTRIBUTE1  Reastreamento
                                           , 'DV_5102_5405_VENDA_MERC'           --INTERFACE_LINE_ATTRIBUTE2  Tipo de NF
                                           , r_item.organizacao_venda            --INTERFACE_LINE_ATTRIBUTE3  Cumpo
                                           , r_item.sequencial                   --INTERFACE_LINE_ATTRIBUTE4  Sequencial linha barramento
                                           , p_batch_source_name                 --batch_source_name
                                           , p_set_of_books_id                   --set_of_books_id
                                           , 'LINE'                              --line_type
                                           , r_item.item_descricao               --description
                                           , 'BRL'                               --currency_code
                                           , r_item.valor_item --r_item.valor_item                   --amount
                                           , p_cust_trx_type_id                  --cust_trx_type_id
                                           , r_item.data_hora                    --gl_date
                                           , v_term_id --p_term_id --6118                                --term_id **** REVER
                                           , p_cust_account_id                   --orig_system_bill_customer_id
                                           , p_bill_to                           --orig_system_bill_address_id
                                           , p_cust_account_id                   --orig_system_ship_customer_id
                                           , p_ship_to                           --orig_system_ship_address_id
                                           , p_cust_account_id                   --orig_system_sold_customer_id
                                           , 'User'                              --conversion_type
                                           , 1                                   --conversion_rate
                                           , v_trx_number --r_item.cupom_venda || '-'|| r_item.estacao                  --trx_number
                                           , 1 --r_item.quantidade                    --quantity
                                           , r_item.valor_item --( (r_item.valor_item + nvl(r_item.valor_desconto,0))  / r_item.quantidade) --unit_selling_price
                                           , r_item.item_id                      --inventory_item_id
                                           , p_pbm_empresa_cliente               --header_attribute4 cliente
                                           , p_pbm_autorizacao                   --header_attribute6
                                           , r_item.estacao                      --header_attribute7 filial
                                           , r_item.cupom_venda                  --header_attribute10
                                           , p_autoricazao                       --header_attribute11 codigo autoriza?Êo
                                           , r_item.uom_code                     --uom_code
                                           ,  fnd_global.user_id                 --created_by
                                           , sysdate                             --creation_date
                                           , fnd_global.user_id                  --last_updated_by
                                           , sysdate                             --last_update_date
                                           , fnd_global.login_id                 --last_update_login
                                           , p_operating_unit                    --org_id
                                           , 'N'                                 --amount_includes_tax_flag
                                           , 'JL.BR.ARXTWMAI.Additional Info'    --header_gdf_attr_category  --JL.BR.ARXTWMAI.Additional Info
                                           , 'JL.BR.ARXTWMAI.Additional Info'   --line_gdf_attr_category
                                           , r_item.ncm                         --line_gdf_attribute2
                                           , r_item.global_attribute2           --line_gdf_attribute3
                                           , r_item.origem                      --line_gdf_attribute4
                                           , r_item.tipo_fiscal                 --line_gdf_attribute5
                                           , r_item.sit_federal                 --line_gdf_attribute6
                                           , r_item.sit_estadual                --line_gdf_attribute7
                                           , p_organization_id                  --warehouse_id
                                           , r_item.data_hora                   --ship_date_actual
                                           , r_item.valor_item                  -- attribute9
                                            );
          --desconto --
          /*
          if nvl(r_item.valor_desconto,0) > 0 then
            insert into ar.ra_interface_lines_all(interface_line_context
                                           , interface_line_attribute1
                                           , interface_line_attribute2
                                           , interface_line_attribute3
                                           , interface_line_attribute4
                                           , batch_source_name
                                           , set_of_books_id
                                           , line_type
                                           , description
                                           , currency_code
                                           , amount
                                           , cust_trx_type_id
                                           , gl_date
                                           , term_id
                                           , orig_system_bill_customer_id
                                           , orig_system_bill_address_id
                                           , orig_system_ship_customer_id
                                           , orig_system_ship_address_id
                                           , orig_system_sold_customer_id
                                           , conversion_type
                                           , conversion_rate
                                           , trx_number
                                           , quantity
                                           , unit_selling_price
                                           , inventory_item_id
                                           , header_attribute4
                                           , header_attribute6
                                           , header_attribute7
                                           , header_attribute10
                                           , header_attribute11
                                           , uom_code
                                           , created_by
                                           , creation_date
                                           , last_updated_by
                                           , last_update_date
                                           , last_update_login
                                           , org_id
                                           , amount_includes_tax_flag
                                           , header_gdf_attr_category  --JL.BR.ARXTWMAI.Additional Info
                                           , line_gdf_attr_category
                                           , line_gdf_attribute2
                                           , line_gdf_attribute3
                                           , line_gdf_attribute4
                                           , line_gdf_attribute5
                                           , line_gdf_attribute6
                                           , line_gdf_attribute7
                                           , warehouse_id
                                           , ship_date_actual
                                            ) values (
                                             'ANALISA'                           --INTERFACE_LINE_CONTEXT     Sistema Legado
                                           , v_ident_cupom                       --INTERFACE_LINE_ATTRIBUTE1  Reastreamento
                                           , 'DV_5102_5405_VENDA_MERC'           --INTERFACE_LINE_ATTRIBUTE2  Tipo de NF
                                           , r_item.organizacao_venda            --INTERFACE_LINE_ATTRIBUTE3  Cumpo
                                           , (r_item.sequencial ||'.2')                   --INTERFACE_LINE_ATTRIBUTE4  Sequencial linha barramento
                                           , p_batch_source_name                 --batch_source_name
                                           , p_set_of_books_id                   --set_of_books_id
                                           , 'LINE'                              --line_type
                                           , ('DESCONTO.' || r_item.sequencial)  --description
                                           , 'BRL'                               --currency_code
                                           , (r_item.valor_desconto *-1)         --r_item.valor_item                   --amount
                                           , p_cust_trx_type_id                  --cust_trx_type_id
                                           , r_item.data_hora                    --gl_date
                                           , p_term_id --6118                                --term_id **** REVER
                                           , p_cust_account_id                   --orig_system_bill_customer_id
                                           , p_bill_to                           --orig_system_bill_address_id
                                           , p_cust_account_id                   --orig_system_ship_customer_id
                                           , p_ship_to                           --orig_system_ship_address_id
                                           , p_cust_account_id                   --orig_system_sold_customer_id
                                           , 'User'                              --conversion_type
                                           , 1                                   --conversion_rate
                                           , r_item.cupom_venda || '-'|| r_item.estacao                  --trx_number
                                           , r_item.quantidade                    --quantity
                                           , ((r_item.valor_desconto/ r_item.quantidade) * -1) --unit_selling_price
                                           , r_item.item_id                      --inventory_item_id
                                           , p_pbm_empresa_cliente               --header_attribute4 cliente
                                           , p_pbm_autorizacao                   --header_attribute6
                                           , r_item.estacao                      --header_attribute7 filial
                                           , r_item.cupom_venda                  --header_attribute10
                                           , p_autoricazao                       --header_attribute11 codigo autoriza?Êo
                                           , r_item.uom_code                     --uom_code
                                           ,  fnd_global.user_id                 --created_by
                                           , sysdate                             --creation_date
                                           , fnd_global.user_id                  --last_updated_by
                                           , sysdate                             --last_update_date
                                           , fnd_global.login_id                 --last_update_login
                                           , p_operating_unit                    --org_id
                                           , 'N'                                 --amount_includes_tax_flag
                                           , 'JL.BR.ARXTWMAI.Additional Info'    --header_gdf_attr_category  --JL.BR.ARXTWMAI.Additional Info
                                           , 'JL.BR.ARXTWMAI.Additional Info'   --line_gdf_attr_category
                                           , r_item.ncm                         --line_gdf_attribute2
                                           , r_item.global_attribute2           --line_gdf_attribute3
                                           , r_item.origem                      --line_gdf_attribute4
                                           , r_item.tipo_fiscal                 --line_gdf_attribute5
                                           , r_item.sit_federal                 --line_gdf_attribute6
                                           , r_item.sit_estadual                --line_gdf_attribute7
                                           , p_organization_id                  --warehouse_id
                                           , r_item.data_hora                   --ship_date_actual
                                            );
          end if;
          -- movimenta?Êo estoque
          -- fim movimenta?Êo estoque

        -- gerar imposto se necess?rio.
          if (r_item.icms != 0) and (r_item.icms_aliquota != 0) then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = r_item.icms_aliquota
                 and end_date          is null
                 and org_id          = p_operating_unit
                 and tax             = 'ICMS_C';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code ICMS_C ' ||r_item.icms_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code ICMS_C ' ||r_item.icms_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;

            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount
                                             , attribute1

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.ICMS_C')  --interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'ICMS'                           --description
                                             , 'BRL'                            --currency_code
                                             , r_item.icms                      --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , r_item.icms_aliquota             --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , r_item.icms                      --line_gdf_attribute19
                                             , r_item.icms                      --line_gdf_attribute20
                                             , v_tax_code --'ICMS_20_C'                      --tax_rate_code
                                             , 'ICMS_C'                         --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- attribute1
                                              );

            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = (-1 * r_item.icms_aliquota)
                 and end_date          is null
                 and org_id          = p_operating_unit
                 and tax             = 'ICMS_D';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code ICMS_D ' ||r_item.icms_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code ICMS_D ' ||r_item.icms_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;

            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.ICMS_D')  --interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'ICMS'                           --description
                                             , 'BRL'                            --currency_code
                                             , (-1 *r_item.icms)                      --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , (-1 * r_item.icms_aliquota)             --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                             --, to_char((r_item.valor_item - r_item.valor_desconto), '99999.99') --line_gdf_attribute11
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , (-1 * r_item.icms)                      --line_gdf_attribute19
                                             , (-1 * r_item.icms)                      --line_gdf_attribute20
                                             , v_tax_code --'ICMS_20_D'                      --tax_rate_code
                                             , 'ICMS_D'                         --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                              );

          end if;

          if (r_item.pis != 0) and (r_item.pis_aliquota != 0) then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = r_item.pis_aliquota
                 and end_date          is null
                 and global_attribute6 = '01'
                 and org_id          = p_operating_unit
                 and tax             = 'PIS_C';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code PIS_C ' ||r_item.pis_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code PIS_C ' ||r_item.pis_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;
            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.PIS_C')   --interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'PIS'                            --description
                                             , 'BRL'                            --currency_code
                                             , r_item.pis                       --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , r_item.pis_aliquota              --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                             --, to_char((r_item.valor_item - r_item.valor_desconto), '99999.99') --(r_item.valor_item - r_item.valor_desconto) --line_gdf_attribute11
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , r_item.pis                       --line_gdf_attribute19
                                             , r_item.pis                       --line_gdf_attribute20
                                             , v_tax_code --'PIS_1.65_C'                     --tax_rate_code
                                             , 'PIS_C'                          --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                              );

            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = (-1* r_item.pis_aliquota)
                 and end_date          is null
                 and org_id          = p_operating_unit
                 and global_attribute6 = '01'
                 and tax             = 'PIS_D';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code PIS_D ' ||r_item.pis_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code PIS_D ' ||r_item.pis_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;

            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.PIS_D')   --interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'PIS'                            --description
                                             , 'BRL'                            --currency_code
                                             , (-1 *r_item.pis)                      --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , (-1 * r_item.pis_aliquota)             --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                            -- , to_char((r_item.valor_item - r_item.valor_desconto), '99999.99') ---1* (r_item.valor_item - r_item.valor_desconto) --line_gdf_attribute11
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , (-1 * r_item.pis)                       --line_gdf_attribute19
                                             , (-1 * r_item.pis)                       --line_gdf_attribute20
                                             , v_tax_code --'PIS_1.65_D'                     --tax_rate_code
                                             , 'PIS_D'                          --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                              );

          end if;

          if (r_item.cofins != 0) and (r_item.cofins_aliquota != 0) then
            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = r_item.cofins_aliquota
                 and end_date          is null
                 and global_attribute7 = '01'
                 and org_id          = p_operating_unit
                 and tax             = 'COFINS_C';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code COFINS_C ' ||r_item.cofins_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code COFINS_C ' ||r_item.cofins_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;
            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.COFINS_C')--interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'COFINS'                         --description
                                             , 'BRL'                            --currency_code
                                             , r_item.cofins                    --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , r_item.cofins_aliquota              --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                             --, to_char((r_item.valor_item - r_item.valor_desconto), '99999.99') --(r_item.valor_item - r_item.valor_desconto) --line_gdf_attribute11
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , r_item.cofins                    --line_gdf_attribute19
                                             , r_item.cofins                    --line_gdf_attribute20
                                             , v_tax_code --'COFINS_7.6_C'                   --tax_rate_code
                                             , 'COFINS_C'                       --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                              );

            begin
              select tax_code into v_tax_code
                from ar_vat_tax_all
               where set_of_books_id = p_set_of_books_id
                 and tax_rate        = (-1 * r_item.cofins_aliquota)
                 and end_date          is null
                 and global_attribute7 = '01'
                 and org_id          = p_operating_unit
                 and tax             = 'COFINS_D';
            exception
              when others then
                fnd_file.put_line(fnd_file.log,'Error ao localizar tax_code COFINS_D ' ||r_item.cofins_aliquota || ' '  || sqlerrm );
                ----------dbms_output.put_line('Error ao localizar tax_code COFINS_D ' ||r_item.cofins_aliquota || ' '  || sqlerrm);
                l_error := l_error + 1;
            end;

            insert into ar.ra_interface_lines_all(interface_line_context
                                             , interface_line_attribute1
                                             , interface_line_attribute2
                                             , interface_line_attribute3
                                             , interface_line_attribute4
                                             , batch_source_name
                                             , set_of_books_id
                                             , line_type
                                             , description
                                             , currency_code
                                             , amount
                                             , cust_trx_type_id
                                             , term_id
                                             , orig_system_bill_customer_id
                                             , orig_system_ship_customer_id
                                             , link_to_line_context
                                             , link_to_line_attribute1
                                             , link_to_line_attribute2
                                             , link_to_line_attribute3
                                             , link_to_line_attribute4
                                             , conversion_type
                                             , conversion_rate
                                             , tax_rate
                                             , created_by
                                             , creation_date
                                             , last_updated_by
                                             , last_update_date
                                             , last_update_login
                                             , org_id
                                             , amount_includes_tax_flag
                                             , header_gdf_attr_category
                                             , header_gdf_attribute9
                                             , header_gdf_attribute10
                                             , header_gdf_attribute11
                                             , line_gdf_attr_category
                                             , line_gdf_attribute1
                                             , line_gdf_attribute2
                                             , line_gdf_attribute3
                                             , line_gdf_attribute4
                                             , line_gdf_attribute5
                                             , line_gdf_attribute6
                                             , line_gdf_attribute7
                                             , line_gdf_attribute11
                                             , line_gdf_attribute19
                                             , line_gdf_attribute20
                                             , tax_rate_code
                                             , tax
                                             , tax_regime_code
                                             , tax_status_code
                                             , taxable_amount

                                              ) values(
                                               'ANALISA'                        --interface_line_context
                                             , v_ident_cupom                    --interface_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --interface_line_attribute2
                                             , r_item.organizacao_venda         --INTERFACE_LINE_ATTRIBUTE3
                                             , (r_item.sequencial ||'.COFINS_D')--interface_line_attribute4
                                             , p_batch_source_name              --batch_source_name
                                             , p_set_of_books_id                --set_of_books_id
                                             , 'TAX'                            --line_type
                                             , 'COFINS'                         --description
                                             , 'BRL'                            --currency_code
                                             , (-1 *r_item.cofins)              --amount
                                             , p_cust_trx_type_id               --cust_trx_type_id
                                             , p_term_id                        --term_id
                                             , p_cust_account_id                --orig_system_bill_customer_id
                                             , p_cust_account_id                --orig_system_ship_customer_id
                                             , 'ANALISA'                        --link_to_line_context
                                             , v_ident_cupom                    --link_to_line_attribute1
                                             , 'DV_5102_5405_VENDA_MERC'        --link_to_line_attribute2
                                             , r_item.organizacao_venda         --link_to_line_attribute3
                                             , r_item.sequencial                --link_to_line_attribute4
                                             , 'User'                           --conversion_type
                                             , 1                                --conversion_rate
                                             , (-1 * r_item.cofins_aliquota)    --tax_rate
                                             , fnd_global.user_id               --created_by
                                             , sysdate                          --creation_date
                                             , fnd_global.user_id               --last_updated_by
                                             , sysdate                          --last_update_date
                                             , fnd_global.login_id              --last_update_login
                                             , p_operating_unit                 --org_id
                                             , 'N'                              --amount_includes_tax_flag
                                             , 'JL.BR.ARXTWMAI.Additional Info' --header_gdf_attr_category
                                             , 0                                --header_gdf_attribute9
                                             , 0                                --header_gdf_attribute10
                                             , 0                                --header_gdf_attribute11
                                             , 'JL.BR.ARXTWMAI.Additional Info' --line_gdf_attr_category
                                             , null                             --line_gdf_attribute1 cfop
                                             , r_item.ncm                       --line_gdf_attribute2
                                             , r_item.global_attribute2         --line_gdf_attribute3
                                             , r_item.origem                    --line_gdf_attribute4
                                             , r_item.tipo_fiscal               --line_gdf_attribute5
                                             , r_item.sit_federal               --line_gdf_attribute6
                                             , r_item.sit_estadual              --line_gdf_attribute7
                                             --, to_char((r_item.valor_item - r_item.valor_desconto), '99999.99') ---1 * (r_item.valor_item - r_item.valor_desconto) --line_gdf_attribute11
                                             , to_char((r_item.valor_item - nvl(r_item.valor_desconto,0)) ,'99999.99')--line_gdf_attribute11
                                             , (-1 * r_item.cofins)             --line_gdf_attribute19
                                             , (-1 * r_item.cofins)             --line_gdf_attribute20
                                             , v_tax_code --'COFINS_7.6_D'                   --tax_rate_code
                                             , 'COFINS_D'                       --TAX
                                             , 'BRAZIL-VAT'                     --tax_regime_code
                                             , 'STANDARD'
                                             , (r_item.valor_item - nvl(r_item.valor_desconto,0)) -- taxable_amount
                                              );

          end if;
          */
        exception
          when others then
            l_error := l_error + 1;
            fnd_file.put_line(fnd_file.log,'Error ao tentar inserir a linha na interface ar '  || sqlerrm );
            ----------dbms_output.put_line('Error ao tentar inserir a linha na interface ar ' || sqlerrm );
        -- Fim Imposto
        end;
         ----------dbms_output.put_line('l_error: ' || l_error );
        if l_error = 0 then
          null;
          -- processar movmentacao item
          --fnd_file.put_line(fnd_file.log,'Executando a movimenta?Êo de estoque.');
          -- Verifica se item ? estocav?l
          BEGIN
            SELECT msi.stock_enabled_flag
              INTO v_stock_enabled_flag
              FROM mtl_system_items_b msi
             WHERE msi.inventory_item_id   = r_item.item_id
               AND msi.organization_id     = p_organization_id
               AND msi.stock_enabled_flag != 'N';

            --movimentacao_estoque(r_item.item_id, p_organization_id, r_item.unidade_medida, r_item.data_hora, v_ident_cupom, r_item.cost_sales ,v_distribution_id, r_item.quantidade, p_sequencial, r_item.sequencial, r_item.valor_item);
          EXCEPTION
            WHEN OTHERS THEN
              NULL;
          END;
          --RETURN TRUE;


          --COMMIT;
        else
          fnd_file.put_line(fnd_file.log,'Ocorreu erro efetuou ROLLBACK.');
          rollback;
          --RETURN FALSE;
        end if;
      --header_attribute6 - PBM_AUTORIZACAO
      --header_attribute7   codigo_filial
      --header_attribute10  cupom_venda
      --HEADER_ATTRIBUTE4
      --HEADER_ATTRIBUTE11 codigo autorizacao
    end loop;

    if l_error = 0 then
          null;
          -- processar movmentacao item
          --movimentacao_estoque(r_item.item_id, p_organization_id, r_item.unidade_medida, r_item.data_hora, v_ident_cupom, r_item.cost_sales, r_item.quantidade, p_sequencial, r_item.sequencial);
          commit;
          return true;


        else

          rollback;
          return false;
        end if;

  end processar_cupom_ar;

  procedure movimentacao_estoque (p_item                  in mtl_system_items_b.inventory_item_id%type
                                , p_organization_id       in org_organization_definitions.organization_id%type
                                , p_uom                   in mtl_system_items_b.primary_uom_code%type
                                , p_transaction_date      in mtl_transactions_interface.transaction_date%type
                                , p_transaction_reference in mtl_transactions_interface.transaction_reference%type
                                , p_cost_sales            in mtl_parameters.cost_of_sales_account%type
                                , p_distribution          in mtl_parameters.cost_of_sales_account%type
                                , p_transaction_quantity  in mtl_transactions_interface.transaction_quantity%type
                                , p_id_ped_venda_cab      in number
                                , p_id_ped_venda_lin      in number
                                , p_transaction_cost       in number
                                ) as
  begin
    insert into apps.xxven_inv_estoque_analisa(id_ped_venda_cab
                                            , id_ped_venda_lin
                                            , cupom_venda
                                            , organization_id
                                            , inventory_item_id
                                            , quantity_original
                                            , quantity_mic
                                            , uom_code
                                            , date_sale
                                            , distribution_id
                                            , misc_id
                                            , status
                                            , transaction_cost
                                             )
                                       values(p_id_ped_venda_cab
                                            , p_id_ped_venda_lin
                                            , p_transaction_reference
                                            , p_organization_id
                                            , p_item
                                            , p_transaction_quantity
                                            , 0
                                            , p_uom
                                            , p_transaction_date
                                            , p_cost_sales
                                            , p_distribution
                                            , 'U'
                                            , p_transaction_cost
                                       );
    --fnd_file.put_line(fnd_file.log,'Gravou na interface de movimenta?Êo de estoque.');
    null;
  exception
    when others then
      ----------dbms_output.put_line('Erro ao inserir linhas para movimenta?Êo de invent?rio. ' || sqlcode || ' ' || sqlerrm);
      fnd_file.put_line(fnd_file.log,'Erro ao inserir linhas para movimentação de inventário. ' || sqlcode || ' ' || sqlerrm);
      rollback;
  end movimentacao_estoque;

  function get_balance_item(p_inventory_item_id in mtl_system_items_b.inventory_item_id%type
                          , p_organization_id   in org_organization_definitions.organization_id%type) return number as
    v_qty number;
  BEGIN
    SELECT nvl((SELECT SUM(transaction_quantity)
              FROM mtl_onhand_quantities
             WHERE inventory_item_id = p_inventory_item_id
               AND organization_id = p_organization_id
               AND subinventory_code = 'COMERC.'),0) -
           nvl((SELECT SUM(transaction_quantity)
                 FROM mtl_transactions_interface
                WHERE inventory_item_id = p_inventory_item_id
                  AND organization_id   = p_organization_id
                  AND subinventory_code = 'COMERC.'),0)
      INTO v_qty
      FROM dual;

    RETURN v_qty;
  EXCEPTION
    WHEN no_data_found THEN
      RETURN 0;
  END;

  procedure processar_movimentacao_estoque(p_id_sequencial IN NUMBER) as
    v_balance number := 0;
    v_dif     number := 0;
  begin
    for r_estoque in (select inventory_item_id, organization_id, quantity_original, quantity_mic
                           , uom_code, date_sale, distribution_id, cupom_venda, misc_id, id_ped_venda_cab, transaction_cost
                           , (get_balance_item(inventory_item_id, organization_id)) balance
                        from apps.xxven_inv_estoque_analisa
                       where status = 'U'
                         and id_ped_venda_cab = p_id_sequencial
                      order by organization_id, inventory_item_id ) loop
      begin
        v_balance := r_estoque.balance; --get_balance_item(r_estoque.inventory_item_id
                                       --               , r_estoque.organization_id);

        if v_balance > 0 then
          if (v_balance - r_estoque.quantity_original) >= 0 then
            -- insere movimentacao mtl_transactions_interface
            ----------dbms_output.put_line('Existe saldo em estoque e processou o item: ' || r_estoque.inventory_item_id || ' Qtd.: ' || r_estoque.quantity_original || ' Cupom: ' || r_estoque.cupom_venda);

            movimentacao_estoque_interface(r_estoque.inventory_item_id
                                         , r_estoque.organization_id
                                         , r_estoque.quantity_original
                                         , r_estoque.uom_code
                                         , r_estoque.distribution_id
                                         , r_estoque.misc_id
                                         , r_estoque.cupom_venda
                                         , r_estoque.date_sale
                                         , 0
                                         , NULL);

           --  atualiza tabela customizada
            update apps.xxven_inv_estoque_analisa set status = 'P'
             where inventory_item_id = r_estoque.inventory_item_id
               and organization_id   = r_estoque.organization_id
               and id_ped_venda_cab  = r_estoque.id_ped_venda_cab; --and cupom_venda       = r_estoque.cupom_venda;

          --  COMMIT;
          elsif (v_balance - r_estoque.quantity_original) < 0 then
            ----------dbms_output.put_line('Existe saldo parcial em estoque e processou o item: ' || r_estoque.inventory_item_id || ' Qtd.: ' || r_estoque.quantity_original || ' Cupom: ' || r_estoque.cupom_venda);

            v_dif := 0;
            v_dif := (r_estoque.quantity_original - v_balance);

            movimentacao_estoque_interface(r_estoque.inventory_item_id
                                         , r_estoque.organization_id
                                         , r_estoque.quantity_original
                                         , r_estoque.uom_code
                                         , r_estoque.distribution_id
                                         , r_estoque.misc_id
                                         , r_estoque.cupom_venda
                                         , r_estoque.date_sale
                                         , v_dif
                                         , r_estoque.transaction_cost);

            --v_balance := r_estoque.quantity_original - v_balance;


            update apps.xxven_inv_estoque_analisa set quantity_mic = v_dif, status = 'P'
             where inventory_item_id = r_estoque.inventory_item_id
               and organization_id   = r_estoque.organization_id
               and id_ped_venda_cab  = r_estoque.id_ped_venda_cab; --and cupom_venda       = r_estoque.cupom_venda;


          end if;
        else
          -- Item com saldo zero
          ----------dbms_output.put_line('Sem saldo Item: ' || r_estoque.inventory_item_id || ' Saldo: ' ||  v_balance);

          movimentacao_estoque_interface(r_estoque.inventory_item_id
                                         , r_estoque.organization_id
                                         , r_estoque.quantity_original
                                         , r_estoque.uom_code
                                         , r_estoque.distribution_id
                                         , r_estoque.misc_id
                                         , r_estoque.cupom_venda
                                         , r_estoque.date_sale
                                         , r_estoque.quantity_original
                                         , r_estoque.transaction_cost);

          update apps.xxven_inv_estoque_analisa set quantity_mic = r_estoque.quantity_original, status = 'P'
             where inventory_item_id = r_estoque.inventory_item_id
               and organization_id   = r_estoque.organization_id
               and id_ped_venda_cab  = r_estoque.id_ped_venda_cab; --and cupom_venda       = r_estoque.cupom_venda;
        end if;
      exception
        when others then
          v_balance := 0;

      end;
    end loop;
    /*
    INSERT INTO xxven_inv_estoque_analisa(id_ped_venda_cab
                                        , id_ped_venda_lin
                                        , cupom_venda
                                        , organization_id
                                        , inventory_item_id
                                        , quantity_original
                                        , quantity_remaining
                                        , uom_code
                                        , date_sale
                                        , distribution_id
                                        , status
                                         )
                                   VALUES(p_id_ped_venda_cab
                                        , p_id_ped_venda_lin
                                        , p_transaction_reference
                                        , p_organization_id
                                        , p_item
                                        , p_transaction_quantity
                                        , p_transaction_quantity
                                        , p_uom
                                        , p_transaction_date
                                        , p_cost_sales
                                        , 'M'
                                   );
    */
  end;

  procedure movimentacao_estoque_interface(p_inventory_item_id  in mtl_transactions_interface.inventory_item_id%type
                                         , p_organization_id    in mtl_transactions_interface.organization_id%type
                                         , p_quantity_remaining in mtl_transactions_interface.transaction_quantity%type
                                         , p_uom_code           in mtl_transactions_interface.transaction_uom%type
                                         , p_distribution_id    in mtl_transactions_interface.distribution_account_id%type
                                         , p_misc               in mtl_transactions_interface.distribution_account_id%type
                                         , p_cupom_venda        in mtl_transactions_interface.transaction_reference%type
                                         , p_transaction_date   in mtl_transactions_interface.transaction_date%type
                                         , p_diferenca          in mtl_transactions_interface.transaction_quantity%type
                                         , p_transaction_cost   in mtl_transactions_interface.transaction_cost%type)  as
  begin
    if p_diferenca = 0 then
        insert into inv.mtl_transactions_interface (transaction_interface_id
                                           , source_header_id
                                           , source_line_id
                                           , source_code
                                           , process_flag
                                           , transaction_mode
                                           , lock_flag
                                           , last_update_date
                                           , last_updated_by
                                           , creation_date
                                           , created_by
                                           , inventory_item_id
                                           , organization_id
                                           , transaction_quantity
                                           , transaction_uom
                                           , transaction_date
                                           , subinventory_code
                                           , transaction_type_id
                                           , transaction_source_type_id
                                           , transaction_reference
                                           , distribution_account_id
                                           , validation_required)
                                     values( mtl_material_transactions_s.nextval  --transaction_interface_id
                                           ,1                           --source_header_id
                                           ,1                           --source_line_id
                                           ,'Venda PDV'                 --source_code
                                           , 1                          --process_flag
                                           , 3                          --transaction_mode
                                           , 2                          --lock_flag 1 ou 2
                                           , sysdate                    --last_update_date
                                           , fnd_global.user_id         --last_updated_by
                                           , sysdate                    --creation_date
                                           , fnd_global.user_id         --created_by
                                           , p_inventory_item_id        --inventory_item_id
                                           , p_organization_id          --organization_id
                                           , (-1*p_quantity_remaining)  --transaction_quantity
                                           , p_uom_code                 --transaction_uom
                                           , p_transaction_date         --transaction_date
                                           , 'COMERC.'                  --subinventory_code
                                           , (select transaction_type_id from mtl_transaction_types where transaction_type_name = 'Venda PDV')--101 108                       --transaction_type_id
                                           , 13                         --transaction_source_type_id
                                           , p_cupom_venda              --transaction_reference         cupom
                                           , p_distribution_id          --distribution_account_id       cost sales
                                           , 1                          --VALIDATION_REQUIRED
                                           );
        ----------dbms_output.put_line('Item inserido mtl_transactions_interface.');
      elsif p_diferenca > 0 then
        null;
        ---
        insert into inv.mtl_transactions_interface (transaction_interface_id
                                           , source_header_id
                                           , source_line_id
                                           , source_code
                                           , process_flag
                                           , transaction_mode
                                           , lock_flag
                                           , last_update_date
                                           , last_updated_by
                                           , creation_date
                                           , created_by
                                           , inventory_item_id
                                           , organization_id
                                           , transaction_quantity
                                           , transaction_uom
                                           , transaction_date
                                           , subinventory_code
                                           , transaction_type_id
                                           , transaction_source_type_id
                                           , transaction_reference
                                           , distribution_account_id
                                           , validation_required
                                           , transaction_cost)
                                     values( mtl_material_transactions_s.nextval  --transaction_interface_id
                                           ,1                           --source_header_id
                                           ,1                           --source_line_id
                                           ,'Ajuste Estoque Negativo PDV' --source_code
                                           , 1                          --process_flag
                                           , 3                          --transaction_mode
                                           , 2                          --lock_flag 1 ou 2
                                           , sysdate                    --last_update_date
                                           , fnd_global.user_id         --last_updated_by
                                           , sysdate                    --creation_date
                                           , fnd_global.user_id         --created_by
                                           , p_inventory_item_id        --inventory_item_id
                                           , p_organization_id          --organization_id
                                           , p_diferenca                --transaction_quantity
                                           , p_uom_code                 --transaction_uom
                                           , p_transaction_date         --transaction_date
                                           , 'COMERC.'                  --subinventory_code
                                           , (select transaction_type_id from mtl_transaction_types where transaction_type_name = 'Ajuste Estoque Negativo PDV')--101 108                       --transaction_type_id
                                           , 13                         --transaction_source_type_id
                                           , p_cupom_venda              --transaction_reference         cupom
                                           , p_misc          --distribution_account_id       cost sales
                                           , 1                          --VALIDATION_REQUIRED
                                           , p_transaction_cost
                                           );
        ---
        insert into inv.mtl_transactions_interface (transaction_interface_id
                                           , source_header_id
                                           , source_line_id
                                           , source_code
                                           , process_flag
                                           , transaction_mode
                                           , lock_flag
                                           , last_update_date
                                           , last_updated_by
                                           , creation_date
                                           , created_by
                                           , inventory_item_id
                                           , organization_id
                                           , transaction_quantity
                                           , transaction_uom
                                           , transaction_date
                                           , subinventory_code
                                           , transaction_type_id
                                           , transaction_source_type_id
                                           , transaction_reference
                                           , distribution_account_id
                                           , validation_required
                                           , transaction_cost)
                                     values( mtl_material_transactions_s.nextval  --transaction_interface_id
                                           ,1                           --source_header_id
                                           ,1                           --source_line_id
                                           ,'Venda PDV'                 --source_code
                                           , 1                          --process_flag
                                           , 3                          --transaction_mode
                                           , 2                          --lock_flag 1 ou 2
                                           , sysdate                    --last_update_date
                                           , fnd_global.user_id         --last_updated_by
                                           , sysdate                    --creation_date
                                           , fnd_global.user_id         --created_by
                                           , p_inventory_item_id        --inventory_item_id
                                           , p_organization_id          --organization_id
                                           , (-1 * p_quantity_remaining)  --transaction_quantity
                                           , p_uom_code                 --transaction_uom
                                           , p_transaction_date         --transaction_date
                                           , 'COMERC.'                  --subinventory_code
                                           , (select transaction_type_id from mtl_transaction_types where transaction_type_name = 'Venda PDV')--101 108                       --transaction_type_id
                                           , 13                         --transaction_source_type_id
                                           , p_cupom_venda              --transaction_reference         cupom
                                           , p_distribution_id          --distribution_account_id       cost sales
                                           , 1                          --VALIDATION_REQUIRED
                                           , p_transaction_cost
                                           );
        ----------dbms_output.put_line('Item inserido mtl_transactions_interface.');
      end if;
  exception
    when others then
      ----------dbms_output.put_line('Erro ao inserir movimentacao de estoque. ' || sqlcode || ' ' || sqlerrm);
      fnd_file.put_line(fnd_file.log,'Erro ao inserir movimentacao de estoque. ' || sqlcode || ' ' || sqlerrm);
  end movimentacao_estoque_interface;

  procedure importar_recebimento_equals(errbuf    out varchar2
                                       ,retcode   out number) as

    type rec_csv_column_rec is record(estabelecimento   varchar2(100)
                                    , adiquirente       varchar2(100)
                                    , filial            varchar2(100)
                                    , tipo_movimento    varchar2(100)
                                    , data_movimento    date
                                    , lote_unico        varchar2(100)
                                    , organization_id   number
                                    , parcela           number
                                    , valor_bruto       number
                                    , valor_comissao    number
                                    , valor_liquido     number
                                    , banco             varchar2(20)
                                    , agencia           varchar2(20)
                                    , conta             varchar2(20) 
                                    , credito_debito    varchar2(50)
                                    , bandeira          varchar2(100)
                                    , nome_bandeira     varchar2(100)
                                    , produto           varchar2(10)
                                    , nome_produto      varchar2(100)
                                    , status            varchar2(1));
    type rec_type_record is table of rec_csv_column_rec index by binary_integer;
    rec_recebimento rec_type_record;                                

    w_dir         varchar2(100);
    file_error    number := 0;
    file_handle   utl_file.file_type;
    w_texto       varchar2(1000);
    w_adiquirente varchar2(255); 
    j             number;
    i             number;
    w_path        varchar2(255);
    w_id_remessa  number;
    w_header_id   number;
    w_existe      varchar2(1);
    w_qtd         number;

    function get_line_csv(p_text in varchar2
                        , p_ini  in number
                        , p_fim  in number) return string is
    begin
      if p_fim != 32767 then
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, instr(p_text,';',1, p_fim) - instr(p_text,';',1, p_ini) - 1));
      else
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, p_fim));
      end if;
    exception
      when others then
        return '';
    end get_line_csv;
  begin
    w_dir := 'EQUALS_RECEBIMENTOS';

    begin
      select directory_path
        into w_path
        from all_directories
       where directory_name = w_dir; 

    exception
      when others then
        w_path := null;
    end;

    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,'*************** IMPORTACAO DE RECEBIMENTOS **************');
    fnd_file.put_line(fnd_file.log,'=========================================================');
    fnd_file.put_line(fnd_file.log,' ');

    for r_file in (select column_value as arquivo
                     from table(apps.xmlcsv_lista_arquivo(w_path))
                    where column_value like '%.csv') loop
      fnd_file.put_line(fnd_file.output,'Arquivo sendo processado: ' || r_file.arquivo);
      dbms_output.put_line('Arquivo sendo processado: ' || r_file.arquivo);

      w_id_remessa := 0;

      begin
        select '1'
          into w_existe
          from xxven_ar_rec_headers
         where file_name = r_file.arquivo
           and rownum = 1;

        fnd_file.put_line(fnd_file.output,'Arquivo já importado: ' || r_file.arquivo);
        dbms_output.put_line('Arquivo já importado: ' || r_file.arquivo);

        exit;
      exception
        when others then
          null;
          --fnd_file.put_line(fnd_file.output,'Arquivo já importado: ' || SQLERRM);
      end;

      select xxven_ar_conc_cc_headers_s.nextval into w_header_id from dual;

      if not file_exist_rec(r_file.arquivo) then
        --dbms_output.put_line('Passou');
        insert into xxven_ar_rec_headers(header_id
                                      , process_date
                                      , status
                                      , file_name
                                      , creation_date
                                      , created_by)
                                 values(w_header_id
                                      , sysdate
                                      , 'U'
                                      , r_file.arquivo
                                      , sysdate
                                      , -1 );

        begin

          file_handle := utl_file.fopen(w_dir,r_file.arquivo, 'R',2000);

          begin
            utl_file.get_line(file_handle,w_texto);
            utl_file.get_line(file_handle, w_texto);
            dbms_output.put_line('texto: ' || w_texto);

            w_id_remessa := substr(w_texto,length(w_texto)-15,16);
            fnd_file.put_line(fnd_file.output,'texto ' || w_texto); 
            fnd_file.put_line(fnd_file.output,'ID_REMESSA: ' || substr(w_texto,length(w_texto)-15,16) ); 
          exception
            when others then
              fnd_file.put_line(fnd_file.output,'Erro ao ler primeira linha');
              dbms_output.put_line('Erro ao ler primeira linha');
          end;

        exception
          when utl_file.invalid_operation then
            fnd_file.put_line(fnd_file.output,'Operação inválida no arquivo.');
            file_error := 1;
            dbms_output.put_line('Operação inválida no arquivo.');

          when utl_file.invalid_path then
            fnd_file.put_line(fnd_file.output,'Diretório inválido.');
            file_error := 1;
            dbms_output.put_line('Diretório inválido.');

          when others then
            file_error := 1;
            fnd_file.put_line(fnd_file.output,'erro primeira linha');
            dbms_output.put_line('erro primeira linha');

        end;  

        if utl_file.is_open(file_handle) and (file_error = 0) and VerificaSeExisteRemessa(w_id_remessa,2) then

          update xxven_ar_rec_headers set id_remessa = w_id_remessa 
         where header_id = w_header_id; 


          rec_recebimento.delete;
          j := 0;
          i := 0;

          begin
            loop
              if j != 0 then
                utl_file.get_line(file_handle, w_texto);
              end if;  


              j := j + 1;
              i := i + 1;

              rec_recebimento(j).status := 'U';

              rec_recebimento(j).lote_unico := get_line_csv(w_texto, 10 ,11);
              --dbms_output.put_line('Lote unico: ' || rec_recebimento(j).lote_unico);

              rec_recebimento(j).estabelecimento := get_line_csv(w_texto, 1 ,2);
              --dbms_output.put_line('Estabelecimento: ' || rec_recebimento(j).estabelecimento);

              rec_recebimento(j).adiquirente := get_line_csv(w_texto, 5 ,6);
              w_adiquirente                := upper(rec_recebimento(j).adiquirente);
              --dbms_output.put_line('Adiquirente: ' || w_adiquirente);

              begin
                if upper(w_adiquirente) = 'REDE'     then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_REDE';
                elsif upper(w_adiquirente) = 'AMEX'  then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_AMEX';
                elsif upper(w_adiquirente) = 'CIELO' then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_CIELO';
                elsif upper(w_adiquirente) = 'SODEXO' then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_C_SODEXO';
                end if;

                SELECT description
                     , (select hr.organization_id from hr_all_organization_units hr where hr.name = description)
                  INTO rec_recebimento(j).filial
                     , rec_recebimento(j).organization_id
                  FROM fnd_lookup_values
                 WHERE lookup_type     = w_adiquirente ---'XXVEN_ESTABELE_FILIAL_CC_AMEX'
                   AND (attribute7    = rec_recebimento(j).estabelecimento    or attribute7     = ('00' || rec_recebimento(j).estabelecimento))
                   AND LANGUAGE        = 'PTB' --USERENV('LANG')
                   AND nvl(end_date_active,trunc(sysdate)) >= trunc(sysdate)
                   --AND end_date_active IS NULL
                   AND enabled_flag    = 'Y';

                --dbms_output.put_line('Filial: ' || rec_recebimento(j).filial);
                --dbms_output.put_line('Organization: ' || rec_recebimento(j).organization_id);

              exception
                when others then
                  rec_recebimento(j).filial := null;
              end;

              rec_recebimento(j).tipo_movimento := get_line_csv(w_texto, 6 ,7);
              rec_recebimento(j).data_movimento := to_date(get_line_csv(w_texto, 8 ,9),'YYYYMMDD');
              rec_recebimento(j).parcela        := get_line_csv(w_texto, 11 ,12);
              --dbms_output.put_line('Parcela: ' || rec_recebimento(j).parcela);

              rec_recebimento(j).banco          := get_line_csv(w_texto, 12 ,13);
              rec_recebimento(j).agencia        := get_line_csv(w_texto, 13 ,14);
              rec_recebimento(j).conta          := get_line_csv(w_texto, 14 ,15);
              rec_recebimento(j).credito_debito := get_line_csv(w_texto, 15 ,16);

              rec_recebimento(j).valor_bruto := to_number(replace(get_line_csv(w_texto, 16 ,17),',','.'),'999999999.99');
              --dbms_output.put_line('Valor bruto: ' || rec_recebimento(j).valor_bruto);

              rec_recebimento(j).valor_comissao := to_number(replace(get_line_csv(w_texto, 17 ,18),',','.'),'999999999.99');
              --dbms_output.put_line('Valor comissao: ' || rec_recebimento(j).valor_comissao);

              rec_recebimento(j).valor_liquido := to_number(replace(get_line_csv(w_texto, 18 ,19),',','.'),'999999999.99');
              --dbms_output.put_line('Valor liquido: ' || rec_recebimento(j).valor_liquido);

              rec_recebimento(j).bandeira := get_line_csv(w_texto, 19 ,20);
              --dbms_output.put_line('Valor bandeira: ' || rec_recebimento(j).bandeira);
              rec_recebimento(j).nome_bandeira := get_line_csv(w_texto, 20 ,21);
              rec_recebimento(j).produto := get_line_csv(w_texto, 21 ,22);
              rec_recebimento(j).nome_produto := get_line_csv(w_texto, 22 ,23);

              insert into xxven_ar_rec_lines_tmp(line_id 
                                           , header_id
                                           , estabelecimento   
                                           , adiquirente      
                                           , filial
                                           , organization_id  
                                           , tipo_movimento    
                                           , data_movimento    
                                           , lote_unico        
                                           , parcela           
                                           , banco             
                                           , agencia           
                                           , conta             
                                           , credito_debito  
                                           , bandeira          
                                           , nome_bandeira     
                                           , produto          
                                           , produdo_descricao 
                                           , valor_bruto       
                                           , valor_comissao    
                                           , valor_liquido     
                                           , status

                                             )
                                      values(xxven_ar_conc_cc_lines_s.nextval
                                           , w_header_id
                                           , rec_recebimento(j).estabelecimento
                                           , rec_recebimento(j).adiquirente
                                           , rec_recebimento(j).filial
                                           , rec_recebimento(j).organization_id
                                           , rec_recebimento(j).tipo_movimento
                                           , rec_recebimento(j).data_movimento
                                           , rec_recebimento(j).lote_unico
                                           , to_number(rec_recebimento(j).parcela)
                                           , rec_recebimento(j).banco
                                           , rec_recebimento(j).agencia
                                           , rec_recebimento(j).conta 
                                           , rec_recebimento(j).credito_debito
                                           , rec_recebimento(j).bandeira
                                           , rec_recebimento(j).nome_bandeira
                                           , rec_recebimento(j).produto
                                           , rec_recebimento(j).nome_produto
                                           , rec_recebimento(j).valor_bruto
                                           , rec_recebimento(j).valor_comissao
                                           , rec_recebimento(j).valor_liquido
                                           , 'U'
                                            );

            end loop;
            
          exception
            when others then
              commit;
              CarregarRecebimentosAgrupados(w_header_id);
              
              dbms_output.put_line('Linhas: ' || i);
              fnd_file.put_line(fnd_file.output,'Quantidade linhas importadas: ' || i);
          end;
          
          utl_file.fclose(file_handle);    

        end if; -- deu erro ao tentar abrir o arquivo
      else
        null;
      end if;                                
    end loop; --r_file                
  end;

  procedure CarregarRecebimentosAgrupados(p_header_id IN NUMBER) AS
    
  BEGIN
  
    FOR r_rec_lines IN( --SELECT * 
                        --  FROM (
                          SELECT  oth.estabelecimento, oth.filial, oth.organization_id,oth.data_movimento,oth.lote_unico,oth.agencia,oth.credito_debito, 
                                        oth.bandeira,oth.nome_bandeira,oth.produto,oth.produdo_descricao,oth.adiquirente,oth.banco,oth.tipo_movimento,oth.parcela,
                                        oth.conta,oth.valor_liquido,oth.valor_bruto,oth.valor_comissao
                                  FROM( SELECT  LIN.estabelecimento, 
                                                LIN.filial, 
                                                LIN.organization_id,
                                                LIN.data_movimento, 
                                                LIN.lote_unico,
                                                LIN.agencia,
                                                LIN.credito_debito, 
                                                LIN.bandeira, 
                                                LIN.nome_bandeira, 
                                                LIN.produto, 
                                                LIN.produdo_descricao,
                                                decode( UPPER(LIN.adiquirente), 'AMEX', 'CIELO',UPPER(LIN.adiquirente))  adiquirente, 
                                                LIN.banco, 
                                                lpad(LIN.conta,20,'0') conta,
                                                --ood.operating_unit,
                                                HEA.header_id,
                                                LIN.parcela,
                                                LIN.tipo_movimento,
                                                SUM(LIN.valor_liquido) valor_liquido,
                                                SUM(LIN.valor_bruto) valor_bruto, 
                                                SUM(LIN.valor_comissao) valor_comissao
                                          FROM xxven_ar_rec_headers HEA
                                             , xxven_ar_rec_lines_tmp   LIN
                                             --, org_organization_definitions ood
                                         WHERE HEA.header_id = LIN.header_id
                                           AND HEA.status = 'U'
                                           AND NVL(lin.status,'U') = 'U' 
                                           --AND ood.organization_id = LIN.organization_id
                                           
                                           AND LIN.data_movimento >= TO_DATE('05/07/18','DD/MM/YY') 
                                           AND HEA.header_id = p_header_id
                                GROUP BY  LIN.estabelecimento,LIN.filial,LIN.organization_id,LIN.data_movimento,LIN.lote_unico,LIN.agencia,LIN.credito_debito,LIN.bandeira, 
                                          LIN.nome_bandeira,LIN.produto,LIN.produdo_descricao,decode( UPPER(LIN.adiquirente), 'AMEX', 'CIELO',UPPER(LIN.adiquirente))  , 
                                          LIN.banco,lpad(LIN.conta,20,'0')--,ood.operating_unit
                                          ,HEA.header_id,LIN.parcela,LIN.tipo_movimento) oth
                           
                        ) LOOP
      begin
         
        insert into xxven_ar_rec_lines     (line_id 
                                           , header_id
                                           , estabelecimento   
                                           , adiquirente      
                                           , filial
                                           , organization_id  
                                           , tipo_movimento    
                                           , data_movimento    
                                           , lote_unico        
                                           , parcela           
                                           , banco             
                                           , agencia           
                                           , conta             
                                           , credito_debito  
                                           , bandeira          
                                           , nome_bandeira     
                                           , produto          
                                           , produdo_descricao 
                                           , valor_bruto       
                                           , valor_comissao    
                                           , valor_liquido     
                                           , status

                                             )
                                      values(xxven_ar_conc_cc_lines_s.nextval
                                           , p_header_id
                                           , r_rec_lines.estabelecimento
                                           , r_rec_lines.adiquirente
                                           , r_rec_lines.filial
                                           , r_rec_lines.organization_id
                                           , r_rec_lines.tipo_movimento
                                           , r_rec_lines.data_movimento
                                           , r_rec_lines.lote_unico
                                           , to_number(r_rec_lines.parcela)
                                           , r_rec_lines.banco
                                           , r_rec_lines.agencia
                                           , r_rec_lines.conta 
                                           , r_rec_lines.credito_debito
                                           , r_rec_lines.bandeira
                                           , r_rec_lines.nome_bandeira
                                           , r_rec_lines.produto
                                           , r_rec_lines.produdo_descricao
                                           , r_rec_lines.valor_bruto
                                           , r_rec_lines.valor_comissao
                                           , r_rec_lines.valor_liquido
                                           , 'U'
                                            );

              commit;
               
    EXCEPTION
      WHEN OTHERS THEN
      dbms_output.put_line('Error ao inserir na tabela definitiva: ' || sqlerrm) ; 
      fnd_file.put_line(fnd_file.output,'Error ao inserir na tabela definitiva: ' || sqlerrm );
    END;
    
    -- lima tabela temporária
    --delete from xxven_ar_rec_lines_tmp where header_id = p_header_id;
    END LOOP;
  
  END;

  procedure importar_movimentacao_equals as
    type rec_csv_column_mov is record(estabelecimento   varchar2(100)
                                    , adiquirente       varchar2(100)
                                    , filial            varchar2(100)
                                    , tipo_movimento    varchar2(100)
                                    , data_movimento    date
                                    , lote_unico        varchar2(100)
                                    , produto           varchar2(10)
                                    , organization_id   number
                                    , parcela           number
                                    , valor_bruto       number
                                    , valor_comissao    number
                                    , valor_liquido     number
                                    , bandeira          varchar2(100)
                                    , data_venc_parcela varchar2(8)
                                    , status            varchar2(1));
    type rec_type_record is table of rec_csv_column_mov index by binary_integer;
    rec_movimento rec_type_record;

    v_dir         varchar2(100);
    file_error    number := 0;
    file_handle   utl_file.file_type;
    w_texto       varchar2(1000);
    j             number;
    i             number;
    w_adiquirente varchar2(100);

    function get_line_csv(p_text in varchar2
                        , p_ini  in number
                        , p_fim  in number) return string is
    begin
      if p_fim != 32767 then
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, instr(p_text,';',1, p_fim) - instr(p_text,';',1, p_ini) - 1));
      else
        return trim(substr(p_text, instr(p_text,';',1, p_ini) + 1, p_fim));
      end if;
    exception
      when others then
        return '';
    end get_line_csv;

  begin
    v_dir := 'EQUAL_MOVIMENTACAO';

    for r_file in (select column_value as arquivo
                     from table(apps.xmlcsv_lista_arquivo('/cargas/PRD/contasareceber/EQUALS/Movimentacao'))
                    where column_value like '%.csv') loop

      fnd_file.put_line(fnd_file.output,'Arquivo sendo processado: ' || r_file.arquivo);
      dbms_output.put_line('Arquivo sendo processado: ' || r_file.arquivo);

      if file_exist(r_file.arquivo) then
        insert into xxven_ar_mov_header(header_id
                                      , process_date
                                      , status
                                      , file_name
                                      , creation_date
                                      , created_by)
                                 values(xxven_ar_conc_cc_headers_s.nextval
                                      , sysdate
                                      , 'U'
                                      , r_file.arquivo
                                      , sysdate
                                      , -1 );

        begin

          file_handle := utl_file.fopen(v_dir,r_file.arquivo, 'R',2000);
        exception
          when utl_file.invalid_operation then
            fnd_file.put_line(fnd_file.output,'Operação inválida no arquivo.');
            file_error := 1;
            dbms_output.put_line('Operação inválida no arquivo.');

          when utl_file.invalid_path then
            fnd_file.put_line(fnd_file.output,'Diretório inválido.');
            file_error := 1;
            dbms_output.put_line('Diretório inválido.');

          when others then
            file_error := 1;
            fnd_file.put_line(fnd_file.output,'erro primeira linha');
            dbms_output.put_line('erro primeira linha');

        end;

        if utl_file.is_open(file_handle) and (file_error = 0) then
          begin
            utl_file.get_line(file_handle,w_texto);
            dbms_output.put_line('texto: ' || w_texto);
          exception
            when others then
              fnd_file.put_line(fnd_file.output,'Erro ao ler primeira linha');
              dbms_output.put_line('Erro ao ler primeira linha');
          end;

          rec_movimento.delete;
          j := 0;
          i := 0;

          begin
            loop
              utl_file.get_line(file_handle, w_texto);
              j := j + 1;
              i := i + 1;

              rec_movimento(j).status := 'P';

              rec_movimento(j).lote_unico := get_line_csv(w_texto, 10 ,11);
              dbms_output.put_line('Lote unico: ' || rec_movimento(j).lote_unico);

              rec_movimento(j).parcela := get_line_csv(w_texto, 11 ,12);
              dbms_output.put_line('Parcela: ' || rec_movimento(j).parcela);

              rec_movimento(j).bandeira := get_line_csv(w_texto, 20 ,21);
              dbms_output.put_line('Bandeira: ' || rec_movimento(j).bandeira);

              rec_movimento(j).produto := get_line_csv(w_texto, 22 ,23);
              dbms_output.put_line('Produto: ' || rec_movimento(j).produto);

              rec_movimento(j).adiquirente := get_line_csv(w_texto, 5 ,6);
              w_adiquirente                := upper(rec_movimento(j).adiquirente);
              dbms_output.put_line('Adiquirente: ' || w_adiquirente);

              rec_movimento(j).estabelecimento := get_line_csv(w_texto, 1 ,2);
              dbms_output.put_line('Estabelecimento: ' || rec_movimento(j).estabelecimento);

              begin
                if upper(w_adiquirente) = 'REDE'     then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_REDE';
                elsif upper(w_adiquirente) = 'AMEX'  then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_AMEX';
                elsif upper(w_adiquirente) = 'CIELO' then
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_CIELO';
                elsif upper(w_adiquirente) = 'SODEXO' then  
                  w_adiquirente := 'XXVEN_ESTABELE_FILIAL_CC_CIELO';
                end if;

                dbms_output.put_line('w_adiquirente: ' || w_adiquirente);

                SELECT description
                     , (select hr.organization_id from hr_all_organization_units hr where hr.name = description)
                  INTO rec_movimento(j).filial
                     , rec_movimento(j).organization_id
                  FROM fnd_lookup_values
                 WHERE lookup_type     = w_adiquirente ---'XXVEN_ESTABELE_FILIAL_CC_AMEX'
                   AND attribute7     = rec_movimento(j).estabelecimento
                   AND LANGUAGE        = 'PTB' --USERENV('LANG')
                   AND nvl(end_date_active,trunc(sysdate)) >= trunc(sysdate)
                   --AND end_date_active IS NULL
                   AND enabled_flag    = 'Y';



                dbms_output.put_line('Filial: ' || rec_movimento(j).filial);
                dbms_output.put_line('Organization: ' || rec_movimento(j).organization_id);
              exception
                when others then
                  rec_movimento(j).filial := null;
              end;

              begin
                rec_movimento(j).data_movimento := to_date(get_line_csv(w_texto, 8 ,9), 'YYYYMMDD');
                dbms_output.put_line('Data Movimento: ' || rec_movimento(j).data_movimento);

                rec_movimento(j).data_venc_parcela := get_line_csv(w_texto, 33 ,34);
                dbms_output.put_line('Data venc parcela: ' || to_date(rec_movimento(j).data_venc_parcela, 'YYYYMMDD')) ;

              exception
                when others then
                  rec_movimento(j).status := 'E';
              end;

              rec_movimento(j).valor_bruto := to_number(replace(get_line_csv(w_texto, 16 ,17),',','.'),'99999.99');
              dbms_output.put_line('Valor bruto: ' || rec_movimento(j).valor_bruto);

              rec_movimento(j).valor_comissao := to_number(replace(get_line_csv(w_texto, 17 ,18),',','.'),'99999.99');
              dbms_output.put_line('Valor comissao: ' || rec_movimento(j).valor_comissao);

              rec_movimento(j).valor_liquido := to_number(replace(get_line_csv(w_texto, 18 ,19),',','.'),'99999.99');
              dbms_output.put_line('Valor liquido: ' || rec_movimento(j).valor_liquido);

              insert into xxven_ar_mov_lines(line_id
                                           , header_id
                                           , estabelecimento
                                           , adiquirente
                                           , filial
                                           , tipo_movimento
                                           , data_movimento
                                           , lote_unico
                                           , parcela
                                           , valor_bruto
                                           , valor_comissao
                                           , valor_liquido
                                           , bandeira
                                           , produto
                                           , data_venc_parcela
                                           , status
                                           , organization_id
                                           , desc_status
                                             )
                                      values(xxven_ar_conc_cc_lines_s.nextval
                                           , xxven_ar_conc_cc_headers_s.currval
                                           , rec_movimento(j).estabelecimento
                                           , rec_movimento(j).adiquirente
                                           , rec_movimento(j).filial
                                           , rec_movimento(j).tipo_movimento
                                           , rec_movimento(j).data_movimento
                                           , rec_movimento(j).lote_unico
                                           , rec_movimento(j).parcela
                                           , rec_movimento(j).valor_bruto
                                           , rec_movimento(j).valor_comissao
                                           , rec_movimento(j).valor_liquido
                                           , rec_movimento(j).bandeira
                                           , rec_movimento(j).produto
                                           , rec_movimento(j).data_venc_parcela
                                           , rec_movimento(j).status
                                           , rec_movimento(j).organization_id
                                           , null
                                            );
              commit;

            end loop;

          exception
            when others then
              dbms_output.put_line('Linhas: ' || i);
              fnd_file.put_line(fnd_file.output,'Quantidade linhas importadas: ' || i);
          end;
          utl_file.fclose(file_handle);

        end if;
      else
        dbms_output.put_line('Arquivo já importado!');
        fnd_file.put_line(fnd_file.output,'Arquivo já importado!');
      end if;
    end loop;
  end;
END XXVEN_AR_CUPOM_ANALISA_PK2;
/