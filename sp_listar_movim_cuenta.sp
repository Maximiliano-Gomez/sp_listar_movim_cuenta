use cob_cuentas
go

if object_id('sp_listar_movim_cuenta') is not null 
begin 
   drop procedure sp_listar_movim_cuenta
end 
go 

/*<summary>
Nombre Fisico: sp_listar_movim_cuenta.sp

CONSULTA DE MOVIMIENTOS DE CUENTA CORRIENTE PARA UN PERIODO ESPECIFICO Y CON LA POSIBILIDAD DE VER MOVIMIENTOS REVERSADOS.

</summary>*/ 

/*<historylog>
<log LogType="Refactor" revision="1.0" date="20/09/2021" email="maximiliano.gomez@accusys.com.ar">AST 63653 - SERVICIO LISTAR MOVIMIENTOS DE 1 CUENTA CORRIENTE</log>
</historylog>*/

create procedure sp_listar_movim_cuenta(
--<parameters>
@s_ssn                    int                            , --<param required ="si"     description="NUMERO TRANSACCIONAL UNICO DADO POR EL MONITOR TRANSACCIONAL PARA LA PRESENTE TRANSACCION."/>
@s_user                   varchar(14)                    , --<param required ="si"     description="USUARIO QUE EJECUTA EL SP."/>
@s_date                   datetime                       , --<param required ="si"     description="FECHA DE EJECUCION DEL SP."/>
@t_trn                    int                            , --<param required ="si"     description="CODIGO UNICO DE TRANSACCION COBIS."/>
@t_debug                  char(1)                  = 'N' , --<param required ="si"     description="MODO DEBUG."/>
@i_quien_llama            char(1)                  = 'F' , --<param required ="no"     description="MARCA DE EJECUCION."/>
@i_n_cuenta_cobis         char(16)                 = null, --<param required ="no"     description="NUMERO DE CUENTA DEL CLEINTE COBIS."/>
@i_n_cbu                  varchar(22)              = null, --<param required ="no"     description="NUMERO DE CUENTA DEL CLEINTE COBIS."/>
@i_f_desde_movim          datetime                 = null, --<param required ="no"     description="FECHA DE FILTRO DE MOVIMIENTOS DESDE."/>
@i_f_hasta_movim          datetime                 = null, --<param required ="no"     description="FECHA DE FILTRO DE MOVIMIENTOS HASTA."/>
@i_m_ver_reversados       char(1)                  = 'N' , --<param required ="no"     description="VER MOVIMIENTOS REVERSADOS S-SI N-NO."/>
-- -----------LOGICA SIGUIENTE---------------------------  
@i_s_movim_hasta          int                      = null, --<param required ="no"     description="SECUENCIAL DE MOVIMIENTO PARA SIGUIENTE."/>
@i_f_movim_hasta          datetime                 = null, --<param required ="no"     description="FECHA VALOR DE MOVIMIENTO PARA SIGUIENTE."/>
@i_f_valor_hasta          datetime                 = null, --<param required ="no"     description="FECHA DE MOVIMIENTO PARA SIGUIENTE."/>
@i_s_valor_hasta          int                      = null, --<param required ="no"     description="SECUENCIAL DE MOVIMIENTO FECHA VALOR PARA SIGUIENTE."/>
@i_m_tipo_movim           char(2)                  = null , --<param required ="no"     description="TIPO DEL ULTIMO MOVIMIENTO VISUALIZADO PARA EL SIGUIENTE: D-DIARIO H-HISTORICO 7-7X24."/>
--------------CANTIDAD DE FILAS A LISTAR------------------
@i_k_filas                int                      = 22   , --<param required="no"      description="NUMERO DE FILAS DEL RESULTSET QUE COBIS SOPORTA."/>

------------OUTPUT---------------------------------------
@o_n_cuenta_pbf           char(19)                 = null output --<param required ="si"     description="NUMERO DE CUENTA PBF (NRO DE CUENTA SEGUN LINK)"/> 
--</parameters>
)
as
declare
@w_n_error int ,
@w_m_genera_consulta   char(1)        ,
@w_k_registros         int            ,
@w_sp_name             varchar(30)    ,
@w_d_mensaje           varchar(180)   ,
@w_c_return            int            ,
@w_c_tran              int            ,
@w_k_min_tope_consulta tinyint        ,    
@w_n_dia_tope_consulta smallint       ,    
@w_i_consulta_activa   int            ,  
@w_f_cons_actual       datetime       ,    
@w_m_movim_diario      char(1)        ,            
@w_m_movim_hist        char(1)        ,
@w_m_movim_7x24        char(1)        ,
@w_m_link7x24          char(1)        ,   
@w_f_proceso           datetime       ,
@w_k_total_registros   int            ,
@w_n_desde_registro    int            ,
@w_n_hasta_registro    int            ,
@w_d_msg               varchar(100)   ,
@w_k_registros_tmp     int            ,
@w_n_cta_banco_cobis   char(16)       ,
@w_n_cuenta_cobis      int            ,
@w_c_oficina_cta       smallint       ,
@w_k_reg_link7x24      int            ,
@w_n_transac_cobis7x24 int            ,
@w_n_causal_cobis7x24  varchar(5)     ,
@w_d_concepto7x24      varchar(60)    ,
@w_m_sigue             char(1)        ,
@w_n_reg_actual        int            ,
@w_n_reg_anterior      int            ,
@w_cde_ente            char(4)        ,
@w_cde_ente_serv       char(3)        ,
@w_fecha_negocio       smalldatetime  ,
@w_itran_link          char(6)        ,
@w_canal               varchar(3)     ,
@w_itrn_interb         char(254)      ,
@w_tipo_extr           char(1)        ,
@w_tipo_dep            char(1)        ,
@w_cbu                 varchar(22)    ,
@w_icuenta             char(30)       ,
@w_icuenta2            char(30)       ,
@w_tran_link           char(6)        ,
@w_tip_cta_ppal        char(1)        ,
@w_producto            tinyint        ,
@w_tran_deb            varchar(5)     ,
@w_tran_cred           varchar(5)     ,
@w_fiid_1              char(4)        ,
@w_typ_1               char(2)        ,
@w_acct_num_1          char(19)       ,
@w_fiid_2              char(4)        ,
@w_typ_2               char(2)        ,
@w_acct_num_2          char(19)       ,
@w_mismo_banco         char(1)        ,
@w_mismo_titular       char(1)        ,
@w_cliente_1           varchar(15)    ,
@w_cliente_2           varchar(15)    ,
@w_canal2              varchar(3)     ,
@w_tipo_cta_desde      char(1)        ,
@w_tipo_cta_hasta      char(1)        ,
@w_mov_moneda          char(2)        ,
@w_tran_cobis          smallint       ,
@w_n_modulo_cuenta_cobro tinyint      ,
@w_n_modulo_cuenta_pago  tinyint      ,
@w_tip_cta_scd         char(1)        ,
@w_tran_cobis_destino  smallint       ,
@w_producto_destino    tinyint        ,
@w_cuenta_aux          char(30)       ,
@w_causa               varchar(5)     ,
@w_n_tabla_codent      smallint       ,
@w_n_tabla_conpei      smallint       ,
@w_ipauthdat           varchar(680)   ,
@w_iloc_term           varchar(40)    ,
@w_concepto            varchar(60)    ,
@w_d_concepto          varchar(60)    ,
@w_tipo_oper           char(1)        ,
@w_codigo_cliente      char(19)       ,
@w_c_motivo            char(3)        ,
@w_d_nombre_fan        char(50)       ,
@w_d_razon_social      char(50)       ,
@w_term_owner_name     char(21)       ,
@w_term_city           char(11)       ,
@w_longitud            int            ,
@w_i                   int            ,
@w_k_paginas_dec       float          ,
@w_k_paginas_ent       int            ,
@w_sev                 int            ,
@w_s_id_registro       int            ,
@w_m_existe            char(1)        ,
@w_n_ente              int            ,
@w_f_tope_persona      datetime       ,
@w_n_cuenta_pbf        char(19)

select 
@w_n_error           = 0,
@w_k_registros       = 0,
@w_sp_name           = 'sp_listar_movim_cuenta'

-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- CONTROLES A PARAMETROS DE ENTRADA - INICIO
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------

-- CONTROL DE CUENTA 
-- VERIFICACION SI NO INGRESA NINGUN NUMERO DE CUENTA--
if  @i_n_cuenta_cobis is null
and @i_n_cbu          is null
begin  
   select 
   @w_n_error   = 708150,
   @w_d_mensaje = 'PARA ESTA CONSULTA ES NECESARIO CONTAR CON ALGUN DATO RELACIONADO A UNA CUENTA'     
   goto error_trap
end  

-- VERIFICACION CUENTA CBU -
if  @i_n_cuenta_cobis is null
and @i_n_cbu          is not null
begin -- SI NO SE ENVIO LA CUENTA COBIS

   select 
   @w_n_cuenta_cobis    = cc_ctacte,
   @w_n_cta_banco_cobis = cc_cta_banco,
   @w_n_ente            = cc_cliente
   from cob_cuentas..cc_ctacte
   where cc_cbu = @i_n_cbu
	  
   if @@rowcount = 0
   begin -- NO EXISTE CUENTA
      select 
      @w_n_error      = 101162, 
      @w_d_mensaje    = 'EL CBU INGRESADO ES INEXISTENTE'
      goto error_trap
   end
end  -- SI NO SE ENVIO LA CUENTA COBIS
else
begin
   select 
   @w_n_cuenta_cobis    = cc_ctacte,
   @w_n_cta_banco_cobis = cc_cta_banco,
   @w_n_ente            = cc_cliente
   from cob_cuentas..cc_ctacte 
   where cc_cta_banco = @i_n_cuenta_cobis
	  
   if @@rowcount = 0
   begin -- NO EXISTE CUENTA
      select 
      @w_n_error      = 101162, 
      @w_d_mensaje    = 'LA CUENTA COBIS INGRESADA ES INEXISTENTE'
      goto error_trap
   end
end

-- CONTROL DE FECHAS
if isnull(@i_f_desde_movim, '01/01/1900') = '01/01/1900' or
   isnull(@i_f_hasta_movim, '01/01/1900') = '01/01/1900'
begin -- ALGUNO DE LOS PARAMETROS FECHA NULOS

   select 
   @w_n_error   = 2630201,   -- Fecha  ingresada es NULL
   @w_d_mensaje = 'DEBE INGRESAR LAS FECHAS DESDE Y HASTA PARA REALIZAR LA CONSULTA'   

   goto error_trap

end   -- ALGUNO DE LOS PARAMETROS FECHA NULOS

if @i_f_desde_movim > @i_f_hasta_movim
begin -- FECHA DESDE MAYOR A FECHA HASTA - ERROR

   select 
   @w_n_error   = 201065,
   @w_d_mensaje = 'LA FECHA DESDE INGRESADA NO PUEDE SER MAYOR A LA FECHA HASTA PARA REALIZAR LA CONSULTA'
   
   goto error_trap

end   -- FECHA DESDE MAYOR A FECHA HASTA - ERROR

--OBTENIENDO FECHA PROCESO
select @w_f_proceso = fp_fecha
from cobis..ba_fecha_proceso

if @i_f_hasta_movim > @w_f_proceso
begin -- FECHA HASTA POSTERIOR A LA FECHA DE PROCESO - ERROR

   select 
   @w_n_error   = 201065,
   @w_d_mensaje = 'LA FECHA HASTA INGRESADA NO PUEDE SER MAYOR A LA FECHA DEL DIA PARA REALIZAR LA CONSULTA'
   
   goto error_trap

end   -- FECHA HASTA POSTERIOR A LA FECHA DE PROCESO - ERROR

select
@w_m_movim_diario = 'N',     
@w_m_movim_hist   = 'N',
@w_m_movim_7x24   = 'N'

-- --------------------------- --
-- FLAG DE MOVIMIENTOS DIARIOS --
-- --------------------------- --
if @i_f_desde_movim = @i_f_hasta_movim and @i_f_desde_movim = convert(char(10),@w_f_proceso,101)
begin
   select @w_m_movim_diario = 'S'
end
if @i_f_desde_movim <= @w_f_proceso and @i_f_hasta_movim >= @w_f_proceso
begin
   select @w_m_movim_diario = 'S'
end

------------------------------ --
-- FLAG DE MOVIMIENTOS HISTORICOS --
------------------------------ --
if @i_f_desde_movim < @w_f_proceso or @i_f_hasta_movim < @w_f_proceso
begin
   select @w_m_movim_hist = 'S'
end
-- ------------------------ --
-- FLAG DE MOVIMIENTOS 7X24 --
-- ------------------------ --
select @w_m_link7x24 = pa_char
from cobis..cl_parametro 
where pa_nemonico  = 'B7X24'

select @w_m_link7x24 = isnull(@w_m_link7x24,'N')

if @w_m_movim_diario = 'S' and @w_m_link7x24 = 'S'     
begin
   select @w_m_movim_7x24 = 'S'          
end

-- -------------------------------------------
-- PARAMETRO: CANTIDAD DE DIAS DE LA CONSULTA
-- -------------------------------------------
if exists( select 1
           from cobis..cl_ente
           where en_ente    = @w_n_ente
           and   en_subtipo = 'P' )
begin -- TIPO PERSONA FISICA

   select @w_n_dia_tope_consulta = pa_smallint
   from cobis..cl_parametro 
   where pa_nemonico  = 'KDCFCF'
   and   pa_producto  = 'CTE'

end   -- TIPO PERSONA FISICA
else
begin -- TIPO PERSONA JURIDICA

   select @w_n_dia_tope_consulta = pa_smallint
   from cobis..cl_parametro 
   where pa_nemonico  = 'KDCFCJ'
   and   pa_producto  = 'CTE'

end   -- TIPO PERSONA JURIDICA

-- DEFAULT PARA CANTIDAD DE DIAS A CONSULTAR
select @w_n_dia_tope_consulta = isnull(@w_n_dia_tope_consulta, 360),
       @w_f_tope_persona      = dateadd(day,@w_n_dia_tope_consulta*-1,@w_f_proceso)

if @i_f_desde_movim < @w_f_tope_persona
begin
   select 
   @w_n_error   = 701302,   -- NUMERO DE DIAS SUPERA EL PARAMETRO 
   @w_d_mensaje = 'LAS FECHAS EXCEDEN LA CANTIDAD DE DIAS MAXIMOS PERMITIDOS (' +  convert(varchar(5),@w_n_dia_tope_consulta) + ')'

   goto error_trap
end


-- ----------------------------------------
-- CONTROL DE REVERSAS
-- ----------------------------------------
if @i_m_ver_reversados not in ( 'S', 'N' )
begin -- CARACTER INVALIDO - ERROR

   select 
   @w_n_error   = 701302, 
   @w_d_mensaje = 'EL PARAMETRO VER MOVIM.REVERSADOS ADMITE S-SI O N-NO'
   
   goto error_trap
   
end   -- CARACTER INVALIDO - ERROR

-- ------------------------------------
-- CONTROL DE PARAMETROS DE SIGUIENTE
-- ------------------------------------
if    @i_s_movim_hasta is not null
   or @i_f_movim_hasta is not null
   or @i_f_valor_hasta is not null
   or @i_s_valor_hasta is not null
   or @i_m_tipo_movim  is not null
begin -- SI SE INGRESO ALGUN DATO PARA SIGUIENTE - CONTROL DE LOGICA DE SIGUIENTE COBIS

   if    @i_s_movim_hasta is null
      or @i_f_movim_hasta is null
      or @i_f_valor_hasta is null
      or @i_s_valor_hasta is null
      or @i_m_tipo_movim  is null
   begin -- SI ALGUN PARAMETRO ESTA INCOMPLETO - ERROR

      select 
      @w_n_error   = 701302, 
      @w_d_mensaje = 'PARA BUSCAR EL SIGUIENTE DATO COBIS NO ADMITE PARAMETROS INCOMPLETOS'
   
      goto error_trap   

   end   -- SI ALGUN PARAMETRO ESTA INCOMPLETO - ERROR

end   -- SI SE INGRESO ALGUN DATO PARA SIGUIENTE - CONTROL DE LOGICA DE SIGUIENTE COBIS
else
begin
    -- CAPTURO OUTPUT DE CUENTA PBF --
    select @o_n_cuenta_pbf = cob_remesas.dbo.fu_li_formato_cta('C',@w_n_cta_banco_cobis)
           
end

if @t_debug = 'S'
begin
   print ' @w_m_movim_diario <%1!> @w_m_movim_hist <%2!> @w_m_movim_7x24 <%3!> ', @w_m_movim_diario,@w_m_movim_hist,@w_m_movim_7x24
   print ' @w_f_proceso %1!  ', @w_f_proceso
   print ' @i_f_desde_movim %1! @i_f_hasta_movim %2! ', @i_f_desde_movim, @i_f_hasta_movim 
end

-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- CONTROLES A PARAMETROS DE ENTRADA - FIN
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------


-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- SEPARACION DE LA INFORMACION SEGUN FILTROS DEL USUARIO - INICIO
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
--------------------------------------------------------------------------------
-- CREACION DE LA TABLA TEMPORAL DE UNIVERSO DE MOVIMIENTOS DIARIOS E HISTORICOS
--------------------------------------------------------------------------------
create table #cc_t_consulta_mov(
cm_s_orden_consulta      int         identity,
cm_f_movimiento          datetime    not null,
cm_f_movimiento_fv       datetime    not null,
cm_f_movimiento_hora     datetime    not null,
cm_c_tipo_tran           int             null, 
cm_c_causa               varchar(5)      null, 
cm_c_oficina             smallint        null,   
cm_a_tipo                char(1)     not null,
cm_i_movimiento          money       not null,
cm_c_moneda              smallint    not null,    
cm_i_iva_basico          money           null, 
cm_i_iva_percepcion      money           null,
cm_i_iva_adicional       money           null, 
cm_d_concepto            varchar(45)     null, 
cm_c_oficina_cta         smallint        null, 
cm_d_transaccion         varchar(60)     null,  -- CAMPO PARA ARMAR LA DESCRIP. PARA LINK
cm_d_comprob_mov         varchar(45)     null,
cm_s_movimiento          int         not null,
cm_u_usuario             varchar(30) not null,
cm_m_estado              char(1)         null,
cm_m_tipo_movim          char(4)         null,
cm_s_movimiento_fv       int             null ) 

if @@error <> 0
begin --CONTROL DE ERROR

   select 
   @w_n_error   = 308028,    -- ERROR DE CREACION DE TABLA              
   @w_d_mensaje = 'ERROR AL CREAR LA TABLA DE TRABAJO #cc_t_consulta_mov'
   goto error_trap                             

end   --CONTROL DE ERROR

create index cc_t_consulta_mov_k01 on #cc_t_consulta_mov( cm_s_orden_consulta ASC ) 

if @@error <> 0
begin -- CONTROL DE ERROR

   select 
   @w_n_error   = 2900124,    --ERROR EN LA CREACION DEL INDICE PARA LA TABLA TEMPORAL         
   @w_d_mensaje = 'ERROR AL CREAR INDICE cc_t_consulta_mov_k02 SOBRE TABLA TEMPORAL #cc_t_consulta_mov'
   
   goto error_trap

end   -- CONTROL DE ERROR   
--------------------------------------------------------------------------------

-- ---------------------------------------------------------
-- TABLA PARA ORDENAR LA INFORMACION DIARIA E HISTORICA
-- ---------------------------------------------------------
create table #cc_t_consulta_tran(
ct_s_orden_consulta      int         identity,
ct_s_orden_orig          int             null,                                  	
ct_f_movimiento          datetime    not null,
ct_f_movimiento_fv       datetime    not null,
ct_f_movimiento_hora     datetime    not null,
ct_c_tipo_tran           int             null, 
ct_c_causa               varchar(5)      null, 
ct_c_oficina             smallint        null,   
ct_a_tipo                char(1)     not null,
ct_i_movimiento          money       not null,
ct_c_moneda              smallint    not null,    
ct_i_iva_basico          money           null, 
ct_i_iva_percepcion      money           null,
ct_i_iva_adicional       money           null, 
ct_d_concepto            varchar(45)     null, 
ct_c_oficina_cta         smallint        null, 
ct_d_transaccion         varchar(60)     null,  -- CAMPO PARA ARMAR LA DESCRIP. PARA LINK
ct_d_comprob_mov         varchar(45)     null,
ct_s_movimiento          int         not null,
ct_u_usuario             varchar(30) not null,
ct_m_estado              char(1)         null,
ct_m_tipo_movim          char(4)         null,
ct_s_movimiento_fv       int             null ) 
	
if @@error <> 0
begin --CONTROL DE ERROR

   select 
   @w_n_error   = 308028,    -- ERROR DE CREACION DE TABLA              
   @w_d_mensaje = 'ERROR AL CREAR LA TABLA DE TRABAJO #cc_t_consulta_tran'
   goto error_trap                             

end   --CONTROL DE ERROR

create index cc_t_consulta_tran_k01 on #cc_t_consulta_tran( ct_c_tipo_tran, ct_c_causa ) 

if @@error <> 0
begin -- CONTROL DE ERROR

   select 
   @w_n_error   = 2900124,    --ERROR EN LA CREACION DEL INDICE PARA LA TABLA TEMPORAL         
   @w_d_mensaje = 'ERROR AL CREAR INDICE cc_t_consulta_tran_k01 SOBRE TABLA TEMPORAL #cc_t_consulta_tran'
   
   goto error_trap

end   -- CONTROL DE ERROR   

create index cc_t_consulta_tran_k02 on #cc_t_consulta_tran( ct_s_orden_consulta DESC ) 

if @@error <> 0
begin -- CONTROL DE ERROR

   select 
   @w_n_error   = 2900124,    --ERROR EN LA CREACION DEL INDICE PARA LA TABLA TEMPORAL         
   @w_d_mensaje = 'ERROR AL CREAR INDICE cc_t_consulta_tran_k02 SOBRE TABLA TEMPORAL #cc_t_consulta_tran'
   
   goto error_trap

end   -- CONTROL DE ERROR
-- ---------------------------------------------------------   
-- ---------------------------------------------------------
-- TABLA PARA MOVIMIENTOS LINK 7X24
-- ---------------------------------------------------------
create table #cc_t_consulta_mov7x24(
cm7_s_orden_consulta      int         identity,
cm7_f_movimiento          datetime    not null,
cm7_f_movimiento_fv       datetime    not null,
cm7_f_movimiento_hora     datetime    not null,
cm7_c_tipo_tran           int             null, 
cm7_c_causa               varchar(5)      null, 
cm7_c_oficina             smallint        null,   
cm7_a_tipo                char(1)     not null,
cm7_i_movimiento          money       not null,
cm7_c_moneda              smallint    not null,    
cm7_i_iva_basico          money           null, 
cm7_i_iva_percepcion      money           null,
cm7_i_iva_adicional       money           null, 
cm7_d_concepto            varchar(45)     null, 
cm7_c_oficina_cta         smallint        null, 
cm7_d_transaccion         varchar(60)     null,  -- CAMPO PARA ARMAR LA DESCRIP. PARA LINK
cm7_d_comprob_mov         varchar(45)     null,
cm7_s_movimiento          int         not null,
cm7_u_usuario             varchar(30)     null,
cm7_m_estado              char(1)         null,
cm7_m_tipo_movim          char(4)         null,
cm7_s_movimiento_fv       int             null,
cm7_s_cuenta_cobis        int             null,
cm7_n_producto_cobis      int             null,
cm7_i_tran_link           varchar(6)      null,
cm7_i_recibo              varchar(12)     null,
cm7_w_n_tarjeta           varchar(20)     null,
cm7_i_msgtype             varchar(4)      null ) 

if @@error <> 0
begin --CONTROL DE ERROR

   select 
   @w_n_error   = 308028,    -- ERROR DE CREACION DE TABLA              
   @w_d_mensaje = 'ERROR AL CREAR LA TABLA DE TRABAJO #cc_t_consulta_mov7X24'
   goto error_trap                             

end   --CONTROL DE ERROR  

create index cc_t_consulta_mov7x24_k01 on #cc_t_consulta_mov7x24( cm7_s_orden_consulta ASC ) 

if @@error <> 0
begin -- CONTROL DE ERROR

   select
   @w_n_error   = 2900124,    --ERROR EN LA CREACION DEL INDICE PARA LA TABLA TEMPORAL         
   @w_d_mensaje = 'ERROR AL CREAR INDICE cc_t_consulta_mov7x24_k01 SOBRE TABLA TEMPORAL #cc_t_consulta_mov7x24'
   goto error_trap

end   -- CONTROL DE ERROR
---------------------------------------------------------------
-- FIN DE LA CREACION DE LA TABLA TEMPORAL
---------------------------------------------------------------

select @w_k_registros = 0   -- CANTIDAD DE MOVIMIENTOS ENCONTRADOS


-- -----------------------
-- MOMENTO 7X24
-- -----------------------
if @w_m_movim_7x24 = 'S' 
begin -- SI ESTAMOS EN ESTADO 7X24

   if exists( select 1 
              from cob_remesas..li_t_transaccion_off_line 
              where s_cuenta_cobis = @w_n_cuenta_cobis )
   begin -- SI EXISTE ALGUN MOVIMIENTO DE LA CUENTA QUE SE ESTA CONSULTANDO
   	 
      select @w_k_reg_link7x24 = 0

      insert into #cc_t_consulta_mov7x24(                  
      cm7_f_movimiento          ,
      cm7_f_movimiento_fv       ,
      cm7_f_movimiento_hora     ,
      cm7_c_tipo_tran           ,   
      cm7_c_causa               ,
      cm7_c_oficina             ,   
      cm7_a_tipo                ,
      cm7_i_movimiento          ,
      cm7_c_moneda              ,      
      cm7_i_iva_basico          , 
      cm7_i_iva_percepcion      ,
      cm7_i_iva_adicional       , 
      cm7_d_concepto            , 
      cm7_c_oficina_cta         , 
      cm7_d_transaccion         , 
      cm7_d_comprob_mov         ,
      cm7_s_movimiento          ,
      cm7_u_usuario             ,
      cm7_m_estado              ,
      cm7_m_tipo_movim          ,
      cm7_s_movimiento_fv       ,
      cm7_s_cuenta_cobis        ,
      cm7_n_producto_cobis      ,
      cm7_i_tran_link           ,
      cm7_i_recibo              ,
      cm7_w_n_tarjeta           ,
      cm7_i_msgtype             )
      select 
      LNKSALDOS.f_hora,
      LNKSALDOS.f_hora,
      LNKSALDOS.f_hora,
      null,                                                               
      null,                                                               
      s_ofi,
      (case when i_transaccion >= 0 then '+' else '-' end),
      i_transaccion,
      ( case when convert(smallint,i_moneda) = 32  then 80
             when convert(smallint,i_moneda) = 840 then 2
             else convert(smallint,i_moneda)
        end ),
      0.00,
      0.00,
      0.00,
      null,                                                              
      @w_c_oficina_cta,
      null,
      isnull(i_recibo, convert(varchar,LNKSALDOS.s_ssn)),
      LNKSALDOS.s_ssn,
      LNKDETALLE.i_msgtype,                         -- USUARIO - VER DISEÑO ADJUNTO EN EL A10!!!!!!!!!!!!
      'N',                          -- ESTADO: REVERSADO O NO
      '77',                         -- TIPO DE REGISTRO PARA EL SIGUIENTE
      LNKSALDOS.s_ssn,              -- EL SECUENCIAL DE FECHA VALOR ES EL MISMO DEL MOVIMIENTO
      LNKSALDOS.s_cuenta_cobis,  
      LNKSALDOS.n_producto_cobis,
      LNKDETALLE.i_tran_link,     
      LNKDETALLE.i_recibo,        
      LNKDETALLE.w_n_tarjeta,     
      LNKDETALLE.i_msgtype       
      from cob_remesas..li_t_transaccion_off_line LNKSALDOS,
           cob_remesas..li_l_transaccion_off_line LNKDETALLE
      where LNKSALDOS.s_cuenta_cobis = @w_n_cuenta_cobis
      and   LNKDETALLE.s_ssn         = LNKSALDOS.s_ssn
      order by LNKSALDOS.s_ssn ASC       -- SECUENCIAL UNICO DE MOVIMIENTO

      select 
      @w_k_reg_link7x24 = @@rowcount,
      @w_n_error     = @@error

      if @w_n_error <> 0
      begin --CONTROL DE ERROR

         select 
         @w_n_error   = 353021,
         @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_mov7X24'
         goto error_trap                             
      
      end   --CONTROL DE ERROR

     

      if @w_k_reg_link7x24 > 0
      begin -- SI EXISTEN MOVIMIENTOS DE 7X24 PARA MOSTRAR EN LA CONSULTA
      	  
         select @w_n_tabla_codent = codigo
         from cobis..cl_tabla
         where tabla = 'li_codigo_ente' 
         
         select @w_n_tabla_conpei = codigo
         from cobis..cl_tabla t
         where t.tabla = 'li_conceptos_pei'
         
         -- ---------------------------------------------------
         -- ACTUALIZACION DE TRANSACCION Y CAUSAL
         -- ---------------------------------------------------
         select 
         @w_m_sigue        = 'S',
         @w_n_reg_actual   = 0,
         @w_n_reg_anterior = 0
         
         while @w_m_sigue = 'S'
         begin -- CICLO PARA COMPLETAR CODIGO DE TRANSACCION + CAUSAL COBIS + CONCEPTO
         
            set rowcount 1
         
            select
            @w_n_reg_actual = cm7_s_movimiento   --s_ssn
            from #cc_t_consulta_mov7x24
            where cm7_s_movimiento > @w_n_reg_anterior
         
            if @@rowcount = 0
            begin -- NO HAY MAS REGISTROS
         
               set rowcount 0
               goto FIN_7x24
         
            end   -- NO HAY MAS REGISTROS
         
            set rowcount 0
         
            select @w_n_reg_anterior = @w_n_reg_actual
         
            select 
            @w_n_transac_cobis7x24 = null,
            @w_n_causal_cobis7x24  = null,
            @w_d_concepto7x24      = null
            
            -- ----------------------------------------------------------------
            -- TOMAR DATOS DE LA TABLA DE 7x24 PARA ANALISIS DE TRANSACCIONES
            -- ----------------------------------------------------------------
            select
            @w_cde_ente       = substring( i_advrecod, 1, 4 ),
            @w_fecha_negocio  = substring( i_fecha_cap, 1, 2 )+'/'+ substring( i_fecha_cap, 3, 2 )+'/'+ substring( convert(char(10),@w_f_proceso,101), 7, 4 ),
            @w_itran_link     = i_tran_link,
            @w_canal          = cob_remesas.dbo.fu_link_canal( substring(i_dummy1,1,2) ),
            @w_itrn_interb    = i_trn_interb,
            @w_tipo_extr      = substring(i_dummy,16,1),     --OBTENGO EL TIPO DE EXTRACCION (COMUN O PUNTO EFECTIVO)            
            @w_tipo_dep       = substring(i_saldos54,13,1),  --OBTENGO EL TIPO DE DEPOSITO (CON SOBRE O EFECTIVO)  
            @w_cbu            = substring(i_saldos54,1,22),
            @w_icuenta        = i_cuenta,
            @w_icuenta2       = i_cuenta2,
            @w_ipauthdat      = i_pauthdat,
            @w_iloc_term      = i_loc_term,
            @w_cde_ente_serv  = substring(i_saldos54, 1, 3),     -- CODIGO DE ENTE SERVICIO               
            @w_codigo_cliente = substring(i_saldos54, 4, 19)     -- Numero de Cuenta "?"
            from cob_remesas..li_l_transaccion_off_line LNKDETALLE
            where LNKDETALLE.s_ssn = @w_n_reg_actual
            -- ----------------------------------------------------------------
            -- ----------------------------------------------------------------
            
            if substring(@w_itran_link,1,2) = '01'
            begin
               select @w_tran_link = '10' + substring(@w_itran_link,3,len(@w_itran_link)-2)
            end
            else
            begin
               select @w_tran_link = @w_itran_link
            end
            
            -- --------------------------------------
            -- CAUSALES
            -- --------------------------------------
            if substring (@w_tran_link,1,2) in ('88') and @w_canal in ('66','74')
            begin
            
               /* BUSCA CAUSAL PARA DEBITO POR PAGOS AFIP*/
               select @w_tran_deb = tr_causa_db
               from cob_remesas..li_caract_trn
               where tr_cod_trn   = @w_tran_link
               and   tr_canal     = @w_canal
               and   tr_pago_ente = @w_cde_ente
            
               if @@rowcount = 0
               begin
                  /* NUMERO DE TRANSACCION LINK NO VALIDO */
                  select  @w_n_error     = 353053,
                          @w_d_mensaje   = 'NUMERO DE TRANSACCION LINK NO VALIDO'
            
                  goto SIGUIENTE_7X24
               end
            /* BUSCA CAUSAL PARA DEBITO POR PAGOS AFIP*/
            end
            else
            --<EAS: CONTROL DE TRANSACCION BANCA EMPR./HOMEB
            -- 09 - debito
            -- 29 - credito
            if substring (@w_tran_link,1,2) in ('09', '29') and @w_canal in ('66', '74', '28', 'ATM', 'PEI') 
            begin -- substring (@w_tran_link,1,2) in ('09', '29') and @w_canal in ('66', '74', '28', 'ATM', 'PEI')
               select @w_mismo_banco      = null,
                      @w_mismo_titular    = null,
                      @w_tipo_cta_desde   = null,
                      @w_tipo_cta_hasta   = null,
                      @w_cliente_1        = null,
                      @w_cliente_2        = null,
                      @w_typ_1            = null,
                      @w_typ_2            = null
            
               if isnull(@w_itrn_interb, '') <> ''
               begin
                  select @w_fiid_1     = substring(@w_itrn_interb,43,4), --BANCO
                         @w_typ_1      = substring(@w_itrn_interb,47,2), --TIPO DE CUENTA
                         @w_acct_num_1 = cob_remesas.dbo.fu_li_formato_cta('L',substring(@w_itrn_interb,49,19)) --NUMERO DE CUENTA DESDE
            
                  select @w_fiid_2     = substring(@w_itrn_interb,68,4), --BANCO
                         @w_typ_2      = substring(@w_itrn_interb,72,2), --TIPO DE CUENTA
                         @w_acct_num_2 = cob_remesas.dbo.fu_li_formato_cta('L',substring(@w_itrn_interb,74,19)) --NUMERO DE CUENTA HASTA

                  if substring (@w_tran_link,1,2) = '09'
                  begin
                      if @w_fiid_2 = '0029' and @w_typ_2 = '00'
                      begin
                         select @w_typ_2      = @w_typ_1,
                                @w_acct_num_2 = @w_acct_num_1
                      end
                  end
                  else
                  begin
                      if @w_fiid_1 = '0029' and @w_typ_1 = '00'   
                      begin
                         select @w_typ_1      = @w_typ_2,
                                @w_acct_num_1 = @w_acct_num_2
                      end
                  end
            
                  if  substring (@w_tran_link,1,2) = '09' and @w_typ_1 = '00'   
                      select @w_typ_1 = substring(@w_tran_link,3,2)
            
            
                  -- CORRIENTES 07=US$, 20=$
                  -- AHORROS    15=US$, 10=$
                  select @w_tipo_cta_desde = ( case @w_typ_1 when '20' then 'C' 
                                                             when '07' then 'C' 
                                                             when '10' then 'A' 
                                                             when '15' then 'A' 
                                               end )
             
                  if @w_fiid_1 = '0029'  -- CTA 1 BCBA
                  begin
                     if @w_tipo_cta_desde = 'C'
                       select @w_cliente_1 = cc_ced_ruc 
                       from cob_cuentas..cc_ctacte 
                       where cc_cta_banco = substring(@w_acct_num_1,1,15)
                     else
                       select @w_cliente_1 = ah_ced_ruc 
                       from cob_ahorros..ah_cuenta 
                       where ah_cta_banco = substring(@w_acct_num_1,1,15)
                  end
            
                  select @w_tipo_cta_hasta = ( case @w_typ_2 when '20' then 'C' 
                                                             when '07' then 'C' 
                                                             when '10' then 'A' 
                                                             when '15' then 'A' 
                                               end )
            
                  if @w_fiid_2 = '0029'   -- CTA 2 BCBA
                  begin
                     if @w_tipo_cta_hasta = 'C'
                        select @w_cliente_2 = cc_ced_ruc 
                        from cob_cuentas..cc_ctacte 
                        where cc_cta_banco = substring(@w_acct_num_2,1,15)
                     else
                        select @w_cliente_2 = ah_ced_ruc 
                        from cob_ahorros..ah_cuenta 
                        where ah_cta_banco = substring(@w_acct_num_2,1,15)
                  end
            
                  -- VERIFICACION DE MISMO BANCO
                  if @w_fiid_1 = @w_fiid_2
                     select @w_mismo_banco = 'S'
                  else
                  begin
                     select @w_mismo_banco = 'N'
            
                     if @w_tipo_cta_desde = null and @w_fiid_2 = '0029'
                     begin
                        select @w_tipo_cta_desde = @w_tipo_cta_hasta,
                               @w_tipo_cta_hasta = null
                     end
                  end
            
                  -- VERIFICACION DE MISMO TITULAR
                  select @w_mismo_titular = rtrim(substring(@w_itrn_interb,40,1)) 
            
                  if  @w_mismo_titular = 'C'
                        select @w_mismo_titular = 'S'
               end
            
               if @w_canal = 'PEI'     ----- SE AGREGA LA DIFERENCIACION DEL CANAL PEI
               begin
               
                  if substring(@w_tran_link,1,2) = '09'
                  begin
                     select @w_tipo_dep = 'C'
                  end
                  else
                  begin
                     select @w_tipo_dep = 'D'
                  end
            
                  select 
                  @w_tran_deb     = tr_causa_db,
                  @w_tran_cred    = tr_causa_cr,
                  @w_tip_cta_ppal = tr_tip_cta_ppal,
                  @w_mov_moneda   = tr_mov_moneda,
                  @w_tip_cta_scd  = tr_tip_cta_scd
                  from cob_remesas..li_caract_trn
                  where tr_cod_trn       = @w_tran_link
                  and   tr_canal         = @w_canal
                  and   tr_mismo_banco   = isnull(@w_mismo_banco, 'N')
                  and   tr_mismo_titular = isnull(@w_mismo_titular, 'N')
                  and   tr_tip_dep       = @w_tipo_dep
               end
               else
               begin
                  select 
                  @w_tran_deb     = tr_causa_db ,
                  @w_tran_cred    = tr_causa_cr ,
                  @w_tip_cta_ppal = tr_tip_cta_ppal,
                  @w_mov_moneda   = tr_mov_moneda,
                  @w_tip_cta_scd  = tr_tip_cta_scd
                  from cob_remesas..li_caract_trn
                  where tr_cod_trn                 = @w_tran_link
                  and   tr_canal                   = @w_canal
                  and   tr_mismo_banco             = isnull(@w_mismo_banco, 'N')
                  and   tr_mismo_titular           = isnull(@w_mismo_titular, 'N')
                  and   isnull(tr_tip_cta_ppal,'') = isnull(@w_tipo_cta_desde, '')
                  and  (isnull(tr_tip_cta_scd, '') = isnull(@w_tipo_cta_hasta, '') or @w_tipo_cta_hasta is null)
               end
            
               if @@rowcount = 0
               begin
                  /* NUMERO DE TRANSACCION LINK NO VALIDO */
                  --select  
                  --@w_n_error   = 353053,
                  --@w_d_mensaje = 'NUMERO DE TRANSACCION LINK NO VALIDO'
                  
                  goto SIGUIENTE_7X24
               end
            end   -- substring (@w_tran_link,1,2) in ('09', '29') and @w_canal in ('66', '74')
            else
            begin
               -- 10 - debito
               if substring (@w_tran_link,1,2) = '10'             
               begin --EXTRACCION
            
                  if @w_tipo_extr = ' '
                     select @w_tipo_extr = null
            
                  select 
                  @w_tran_deb     = tr_causa_db ,
                  @w_tip_cta_ppal = tr_tip_cta_ppal,
                  @w_mov_moneda   = tr_mov_moneda,
                  @w_tip_cta_scd  = tr_tip_cta_scd
                  from cob_remesas..li_caract_trn
                  where tr_cod_trn = @w_tran_link
                  and   tr_canal   = @w_canal
                  and   tr_tip_dep = @w_tipo_extr                       
            
                  if @@rowcount = 0
                  begin
                     select 
                     @w_tran_deb     = tr_causa_db ,
                     @w_tran_cred    = tr_causa_cr ,
                     @w_tip_cta_ppal = tr_tip_cta_ppal,
                     @w_mov_moneda   = tr_mov_moneda,
                     @w_tip_cta_scd  = tr_tip_cta_scd
                     from cob_remesas..li_caract_trn
                     where tr_cod_trn = @w_tran_link
                     and   tr_canal   = 'ATM'
                     and   tr_tip_dep = @w_tipo_extr
            
                     if @@rowcount = 0
                     begin
                        /* NUMERO DE TRANSACCION LINK NO VALIDO */
                        --select 
                        --@w_n_error    = 353053,
                        --@w_d_mensaje    = 'NUMERO DE TRANSACCION LINK NO VALIDO'
                        
                        goto SIGUIENTE_7X24
                     end
                  end
               end   --EXTRACCION
               else
               -- 21 - credito
               if substring (@w_tran_link,1,2) = '21'                
               begin --DEPOSITOS
            
                  select 
                  @w_tran_deb     = tr_causa_db ,
                  @w_tran_cred    = tr_causa_cr ,
                  @w_tip_cta_ppal = tr_tip_cta_ppal,
                  @w_mov_moneda   = tr_mov_moneda,
                  @w_tip_cta_scd  = tr_tip_cta_scd
                  from  cob_remesas..li_caract_trn
                  where tr_cod_trn = @w_tran_link
                  and   tr_canal   = @w_canal
                  and   tr_tip_dep = @w_tipo_dep
            
                  if @@rowcount = 0
                  begin
                     select 
                     @w_tran_deb     = tr_causa_db ,
                     @w_tran_cred    = tr_causa_cr ,
                     @w_tip_cta_ppal = tr_tip_cta_ppal,
                     @w_mov_moneda   = tr_mov_moneda,
                     @w_tip_cta_scd  = tr_tip_cta_scd
                     from  cob_remesas..li_caract_trn
                     where tr_cod_trn = @w_tran_link
                     and   tr_canal   = 'ATM'
                     and   tr_tip_dep = @w_tipo_dep
            
                     if @@rowcount = 0
                     begin
                        /* NUMERO DE TRANSACCION LINK NO VALIDO */
                        --select 
                        --@w_n_error    = 353053,
                        --@w_d_mensaje    = 'NUMERO DE TRANSACCION LINK NO VALIDO'
                        
                        goto SIGUIENTE_7X24
                     end
                  end
               end --DEPOSITOS
               else
               begin --RESTO DE TRANSACCION (TRANSFERENCIAS, COMPRAS ETC)
            
                  select 
                  @w_tran_deb     = tr_causa_db ,
                  @w_tran_cred    = tr_causa_cr ,
                  @w_tip_cta_ppal = tr_tip_cta_ppal,
                  @w_mov_moneda   = tr_mov_moneda,
                  @w_tip_cta_scd  = tr_tip_cta_scd
                  from  cob_remesas..li_caract_trn
                  where tr_cod_trn = @w_tran_link
                  and   tr_canal   = @w_canal
            
                  if @@rowcount = 0
                  begin
                     select 
                     @w_tran_deb     = tr_causa_db ,
                     @w_tran_cred    = tr_causa_cr ,
                     @w_tip_cta_ppal = tr_tip_cta_ppal,
                     @w_mov_moneda   = tr_mov_moneda,
                     @w_tip_cta_scd  = tr_tip_cta_scd
                     from  cob_remesas..li_caract_trn
                     where tr_cod_trn = @w_tran_link
                     and   tr_canal   = 'ATM'
            
                     if @@rowcount = 0
                     begin
                        /* NUMERO DE TRANSACCION LINK NO VALIDO */
                        --select 
                        --@w_n_error    = 353053,
                        --@w_d_mensaje    = 'NUMERO DE TRANSACCION LINK NO VALIDO'
                        
                        goto SIGUIENTE_7X24
                     end
                  end
               end
            end
            
            /* IDENTIFICACION DEL PRODUCTO COBIS ORIGEN DE LA TRANSACCION LINK SOLICITADA */
            if @w_tip_cta_ppal = 'C'
               select  @w_producto = 3                 /* CUENTA CORRIENTE */
            else
               select  @w_producto = 4                 /* CAJA DE AHORROS */            
            
            -- --------------------------------------
            -- --------------------------------------
            
            -- --------------------------------------
            -- TRANSACCIONES
            -- --------------------------------------
            /* EXTRACCION / COMPRA ATM / PAGO SERVICIOS / TRANSFERENCIA INTERBANCARIA */
            if substring (@w_tran_link,1,2) in ('10', '71','76','78','79', '81','86','87','88','09','17','19')
            begin
               if @w_producto = 3
                  select @w_tran_cobis = 50
               else
                  select @w_tran_cobis = 264
            end
            
            /* ANULACION DE COMPRAS */
            if substring (@w_tran_link,1,2) in ('72', '77', '29')
            begin
               if @w_producto = 3
                  select @w_tran_cobis = 48
               else
                  select @w_tran_cobis = 253
            end
            /* PRESTAMOS PREAPROBADOS */
            else if substring (@w_tran_link,1,2) in ('2P')
            begin -- if substring (@w_tran_link,1,2) in ('2P') ==> PRESTAMOS PREAPROBADOS
               if @w_producto = 3
                  select @w_tran_cobis = 48
               else
                  select @w_tran_cobis = 253
            end
            /* TRANSFERENCIA */
            if substring (@w_tran_link,1,2) in ('40')
            begin -- TRANSFERENCIA ===> if substring (@w_tran_link,1,2) in ('40')
               
               -- SI ES COMPRA VENTA DE MONEDA
               if @w_tran_link in ('401015', '401520', '401510', '402015')
               begin -- if @w_tran_link in ('401015', '401520', '401510', '402015') ===> COMPRA DE MONEDA EXTRANJERA
               
                  if @w_n_modulo_cuenta_cobro = 3
                  begin
                     select @w_tran_cobis = 50
                  end
                  else
                  begin
                     select @w_tran_cobis = 264
                  end
               
                  if @w_n_modulo_cuenta_pago = 3
                  begin
                     select @w_tran_cobis_destino = 48
                  end
                  else
                  begin
                     select @w_tran_cobis_destino = 253
                  end
               
               end   -- if @w_tran_link in ('401015', '401520', '401510', '402015') ===> COMPRA DE MONEDA EXTRANJERA
               else
               begin -- ES UNA TRANSFERENCIA ENTRE CUENTAS DE LA MISMA MONEDA
               
                  if @w_producto = 3
                  begin
                     select @w_tran_cobis = 50
                  end
                  else
                  begin
                     select @w_tran_cobis = 264
                  end
               
                  if @w_producto_destino = 3
                  begin
                     select @w_tran_cobis_destino = 48
                  end
                  else
                  begin
                     select @w_tran_cobis_destino = 253
                  end
                  
               end   -- ES UNA TRANSFERENCIA ENTRE CUENTAS DE LA MISMA MONEDA
               
            end   -- TRANSFERENCIA ===> if substring (@w_tran_link,1,2) in ('40')
            /* TRANSFERENCIA CBU*/
            else if substring (@w_tran_link,1,2) in ('1B')
            begin
            
               select @w_tran_cobis = 431
            
            end
            /* PLAZO FIJO */
            else if substring (@w_tran_link,1,2) in ('18')
            begin
            
               if @w_n_modulo_cuenta_cobro = 3
               begin
                  select @w_tran_cobis = 50
               end
               else
               begin
                  select @w_tran_cobis = 264
               end
            
            end
            else
            -- TRANSACCION DEBITO/CREDITO DEBIN
            if substring (@w_tran_link,1,2) = 'F3' or substring (@w_tran_link,1,2) = 'F4' and @w_canal in ('66', '74')
            begin
            
               if isnull(@w_itrn_interb, '') <> ''
               begin
            
                  if substring (@w_tran_link,1,2) = 'F3'
                  begin
                  	  
                     select @w_cuenta_aux = @w_icuenta
                     select @w_causa      = @w_tran_deb
            
                     if @w_producto = 3
                     begin
                        select @w_tran_cobis = 50 -- ND CC
                     end
                     else
                     begin
                        select @w_tran_cobis = 264 -- ND CA
                     end
                     
                  end -- substring (@w_tran_link,1,2) = 'F3'
                  else
                  begin
                  	  
                     select @w_cuenta_aux = @w_icuenta2    
                     select @w_causa      = @w_tran_cred
            
                     if @w_producto = 3
                     begin
                        select @w_tran_cobis = 48 -- NC CC
                     end
                     else
                     begin
                        select @w_tran_cobis = 253 -- NC CA
                     end
                     
                  end -- substring (@w_tran_link,1,2) = 'F4'
            
               end
            
            end
            -- --------------------------------------
            -- --------------------------------------
            
            -- -----------------------------------------------
            -- CONCEPTO
            -- -----------------------------------------------
            select @w_concepto = null
            
            if substring(@w_itran_link, 1, 2) in( '81','86','87','88')
            begin -- DEL 81 AL 88 - TRANSACCIONES POR PAGO DE SERVICIOS
            	
               select @w_tipo_oper = 'S' -- C=PAGO COMERCIO S=PAGO SERVICIO
            
               /* CARGA LA DESCRIPCION DEL SERVICIO PAGADO */
               if @w_cde_ente_serv is not null
               begin
               	
                  select @w_concepto = substring(valor, 1, 15)
                  from cobis..cl_catalogo
                  where tabla                   = @w_n_tabla_codent
                  and   rtrim(@w_cde_ente_serv) = codigo
                  
                  select @w_concepto = @w_concepto + '-' + @w_codigo_cliente
                  
               end
               
            end   -- DEL 81 AL 88 - TRANSACCIONES POR PAGO DE SERVICIOS
            else
            if substring(@w_itran_link, 1, 2) in('09','29')  
            begin -- IF SUBSTRING(@W_ITRAN_LINK, 1, 2) IN('09','29')  
            	
               if @w_canal = 'PEI'
               begin -- IF @W_CANAL = 'PEI'
               	
                  select 
                  @w_c_motivo       = substring(@w_ipauthdat, 273, 3),
                  @w_d_nombre_fan   = substring(@w_ipauthdat, 598, 50),
                  @w_d_razon_social = substring(@w_ipauthdat, 525, 50)
                  
                  -- SE OBTIENE LA DESCRIPCION DEL CONCEPTO PEI A PARTIR DEL CATALOGO - LI_CONCEPTOS_PEI
                  select @w_concepto = c.valor
                  from cobis..cl_catalogo c
                  where c.tabla  = @w_n_tabla_conpei
                  and   c.codigo = @w_c_motivo
                  and   estado   = 'V'
            
                  -- SE OBTIENE EL CONCEPTO PEI
                  select @w_concepto = @w_concepto + ' ' + isnull(@w_d_nombre_fan,@w_d_razon_social)
                  
               end   -- IF @W_CANAL = 'PEI'
               else
               begin
               	
                  if substring(@w_itran_link, 1, 2) = '09'  -- DEBITO
                  begin
                     select @w_concepto = substring(@w_ipauthdat, 301, 11) + ' ' + substring(@w_ipauthdat, 313, 22)
                  end
                  
                  if substring(@w_itran_link, 1, 2) = '29'  -- CREDITO
                  begin
                     select @w_concepto = substring(@w_ipauthdat, 290, 11) + ' ' + substring(@w_ipauthdat, 241, 22)
                  end
                  
               end
            
            end   -- IF SUBSTRING(@W_ITRAN_LINK, 1, 2) IN('09','29')
            else
            if substring(@w_itran_link, 1, 2) in ('71','72','73','74','75','76','77','78','79','17') 
            begin -- IF SUBSTRING(@W_ITRAN_LINK, 1, 2) IN ('71','72','73','74','75','76','77','78','79','17') 
               
               select @w_term_owner_name = substring (@w_iloc_term,1,21)
               select @w_term_city       = substring (@w_iloc_term,23,11)
               
               if @w_term_owner_name is not null and @w_term_owner_name <> ' '
               begin
                  /* CARGA LA DESCRIPCION DEL COMERCIO */
                  select @w_concepto = substring(@w_term_owner_name, 1, 17)
                  select @w_concepto = rtrim(@w_concepto) + ' - '
               
                  select @w_longitud = datalength(@w_concepto)
                  select @w_longitud = 30 - datalength(@w_concepto)
               
                  /* AGREGA CIUDAD DEL COMERCIO */
                  if @w_term_city is not null
                  begin
                     select @w_concepto = @w_concepto + substring(@w_term_city, 1, @w_longitud)
                  end
               
                  /* BUSCA SI EXISTE UN PIPE Y LO REEMPLAZA POR / */
                  select @w_i = 1
                  while @w_i <= datalength(@w_concepto)
                  begin
                    if substring(@w_concepto, @w_i, 1) = '|'
                       select @w_concepto = stuff(@w_concepto, @w_i, 1, "/")
               
                    select @w_i = @w_i + 1
                  end
               end
            
            end   -- IF SUBSTRING(@W_ITRAN_LINK, 1, 2) IN ('71','72','73','74','75','76','77','78','79','17') 
            else
            if substring(@w_itran_link, 1, 2) in ('F3', 'F4')
            begin -- 'F3', 'F4' -- DEBIN  SP_LI_DEB_CRED_DEBIN
            	  
               select @w_d_concepto = 'DEBIN: ' + substring(@w_itrn_interb,1,32) + ' - ' + substring(@w_itrn_interb,33,3)
               
            end   -- 'F3', 'F4' -- DEBIN  SP_LI_DEB_CRED_DEBIN
            -- -----------------------------------------------
            -- -----------------------------------------------
            
            -- ------------------------------
            -- CAMPOS A ACTUALIZAR DE 7X24
            -- ------------------------------
            select 
            @w_n_transac_cobis7x24 = @w_tran_cobis,
            @w_n_causal_cobis7x24  = (case when isnull(@w_tran_deb,'') = '' then @w_tran_cred else @w_tran_deb end),
            @w_d_concepto7x24      = @w_d_concepto
            -- ------------------------------
            -- -----------------------------------------------
            -- ACTUALIZACION DE TABLA TEMPORAL DE 7X24
            -- -----------------------------------------------
            update #cc_t_consulta_mov7x24 set
            cm7_c_tipo_tran   = @w_n_transac_cobis7x24,         -- CODIGO DE TRANSACCION COBIS
            cm7_c_causa       = @w_n_causal_cobis7x24,          -- CODIGO DE CAUSAL COBIS
            cm7_d_transaccion = @w_d_concepto7x24               -- CONCEPTO COBIS
            where cm7_s_movimiento = @w_n_reg_actual 
            
            if @@error <> 0
            begin -- CONTROL DE ERROR       	 
               select 
               @w_n_error   = 355028,    
               @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_mov7x24'
               goto error_trap                             
            end   -- CONTROL DE ERROR            
            -- -----------------------------------------------
         
SIGUIENTE_7X24:
         
         end   -- CICLO PARA COMPLETAR CODIGO DE TRANSACCION + CAUSAL COBIS + CONCEPTO     
         
FIN_7x24:

         -- ---------------------------------------------------------------------
         -- ANALIZAR SI DEBO OMITIR LAS REVERSAS
         -- ---------------------------------------------------------------------
         if @i_m_ver_reversados = 'N'
         begin -- ELIMINAR MOVIMIENTOS REVERSADOS EN 7X24
            
            -- PASO 1: ELIMINAR TODOS LOS MOVIMIENTOS QUE TENGAN SU REVERSA
            delete #cc_t_consulta_mov7x24
            from #cc_t_consulta_mov7x24 LNK7X24DETA
            where exists( select 1
                          from #cc_t_consulta_mov7x24  LNK7X24DET1
                          where LNK7X24DET1.cm7_s_cuenta_cobis     = LNK7X24DETA.cm7_s_cuenta_cobis        -- INFO DEL MOVIM. QUE DEBEN SER IGUALES ENTRE MOVIM Y REVERSA
                          and   LNK7X24DET1.cm7_n_producto_cobis   = LNK7X24DETA.cm7_n_producto_cobis
                          and   LNK7X24DET1.cm7_i_tran_link        = LNK7X24DETA.cm7_i_tran_link
                          and   LNK7X24DET1.cm7_i_recibo           = LNK7X24DETA.cm7_i_recibo
                          and   LNK7X24DET1.cm7_w_n_tarjeta        = LNK7X24DETA.cm7_w_n_tarjeta
                                                              
                          and   LNK7X24DET1.cm7_i_msgtype             in ('0420','0421')    -- UNA REVERSA
                          and   LNK7X24DETA.cm7_i_msgtype         not in ('0420','0421')    -- MOV. NO REVERSA   
                                                              
                          and   LNK7X24DETA.cm7_s_movimiento       < LNK7X24DET1.cm7_s_movimiento -- EL MOV. REVERSA SEA POSTERIOR EL MOVIM.
            
                          and   abs(LNK7X24DETA.cm7_i_movimiento)  = abs(LNK7X24DET1.cm7_i_movimiento)  -- AMBOS MOVIMIENTOS TIENEN EL MISMO VALOR
                          and   (                                                                 -- EL MOVIM. TIENE QUE TENER SIGNO CONTRARIO A LA REVERSA
                                  ( LNK7X24DETA.cm7_i_movimiento > 0 and LNK7X24DET1.cm7_i_movimiento < 0 ) 
                                  or
                                  ( LNK7X24DETA.cm7_i_movimiento < 0 and LNK7X24DET1.cm7_i_movimiento > 0 ) 
                                )
                        )
            
            if @@error <> 0
            begin --CONTROL DE ERROR
            
               select 
               @w_n_error   = 357006,    
               @w_d_mensaje = 'ERROR AL ELIMINAR EN TABLA DE TRABAJO #cc_t_consulta_mov7x24'
               goto error_trap                             
            
            end   --CONTROL DE ERROR
            
         end   -- ELIMINAR MOVIMIENTOS REVERSADOS EN 7X24
         else
         begin -- SI SE QUIERE VER LOS MOVIMIENTOS REVERSADOS - DEBO MARCARLOS Y QUITAR LAS REVERSAS

            -- LOGICA PARA MARCAR LOS MOVIMIENTOS QUE FUERON REVERSADOS
            -- TOMAR LAS REVERSAS, UBICAR A QUE MOVIMIENTO REVERSA Y MARCAR EL MOV. REVERSADO
            
            -- PASO 1: MARCAR MOVIMIENTOS REVERSADOS, EL USUARIO QUIERE VERLOS
            update #cc_t_consulta_mov7x24 set
            cm7_m_estado = 'S'
            from #cc_t_consulta_mov7x24 LNK7X24DETA
            where exists( select 1
                          from #cc_t_consulta_mov7x24  LNK7X24DET1
                          where LNK7X24DET1.cm7_s_cuenta_cobis     = LNK7X24DETA.cm7_s_cuenta_cobis        -- INFO DEL MOVIM. QUE DEBEN SER IGUALES ENTRE MOVIM Y REVERSA
                          and   LNK7X24DET1.cm7_n_producto_cobis   = LNK7X24DETA.cm7_n_producto_cobis
                          and   LNK7X24DET1.cm7_i_tran_link        = LNK7X24DETA.cm7_i_tran_link
                          and   LNK7X24DET1.cm7_i_recibo           = LNK7X24DETA.cm7_i_recibo
                          and   LNK7X24DET1.cm7_w_n_tarjeta        = LNK7X24DETA.cm7_w_n_tarjeta
                                                              
                          and   LNK7X24DET1.cm7_i_msgtype          in ('0420','0421')    -- UNA REVERSA
                          and   LNK7X24DETA.cm7_i_msgtype      not in ('0420','0421')    -- MOV. NO REVERSA   
                                                              
                          and   LNK7X24DETA.cm7_s_movimiento       < LNK7X24DET1.cm7_s_movimiento -- EL MOV. REVERSA SEA POSTERIOR EL MOVIM.
            
                          and   abs(LNK7X24DETA.cm7_i_movimiento)  = abs(LNK7X24DET1.cm7_i_movimiento)  -- AMBOS MOVIMIENTOS TIENEN EL MISMO VALOR
                          and   (                                                                 -- EL MOVIM. TIENE QUE TENER SIGNO CONTRARIO A LA REVERSA
                                  ( LNK7X24DETA.cm7_i_movimiento > 0 and LNK7X24DET1.cm7_i_movimiento < 0 ) 
                                  or
                                  ( LNK7X24DETA.cm7_i_movimiento < 0 and LNK7X24DET1.cm7_i_movimiento > 0 ) 
                                )
                        )                          

            if @@error <> 0
            begin --CONTROL DE ERROR
            
               select 
               @w_n_error   = 355028,    
               @w_d_mensaje = 'ERROR AL ACTUALIZAR EN TABLA DE TRABAJO #cc_t_consulta_mov7x24'
               goto error_trap                             
            
            end   --CONTROL DE ERROR
         
         end   -- SI SE QUIERE VER LOS MOVIMIENTOS REVERSADOS - DEBO MARCARLOS Y QUITAR LAS REVERSAS
         
         -- PASO 2: ELIMINAR TODOS LOS MOVIMIENTOS QUE SON REVERSAS
         delete #cc_t_consulta_mov7x24
         from #cc_t_consulta_mov7x24
         where cm7_i_msgtype in ('0420','0421')  -- REVERSAS
         
         if @@error <> 0
         begin --CONTROL DE ERROR
         
            select 
            @w_n_error   = 357006,    
            @w_d_mensaje = 'ERROR AL ELIMINAR EN TABLA DE TRABAJO #cc_t_consulta_mov7x24'
            goto error_trap                             
         
         end   --CONTROL DE ERROR                        
         -- ---------------------------------------------------------------------
         -- ---------------------------------------------------------------------

         -- CONTADOR GENERAL DE CANTIDAD DE MOVIMIENTOS
         select @w_k_reg_link7x24 = count(1) 
	     from   #cc_t_consulta_mov7x24
		 
         select @w_k_registros = @w_k_registros + @w_k_reg_link7x24
      
      end   -- SI EXISTEN MOVIMIENTOS DE 7X24 PARA MOSTRAR EN LA CONSULTA
      
   end   -- SI EXISTE ALGUN MOVIMIENTO DE LA CUENTA QUE SE ESTA CONSULTANDO

end   -- SI ESTAMOS EN ESTADO 7X24
-- -----------------------
-- -----------------------
-- ----------------------------------------------------------
-- MOVIMIENTOS DIARIOS
-- ---------------------------------------------------------
if @w_m_movim_diario = 'S'
begin -- OPERATIVO

   insert into #cc_t_consulta_mov(                  
   cm_f_movimiento          ,
   cm_f_movimiento_fv       ,
   cm_f_movimiento_hora     ,
   cm_c_tipo_tran           ,   
   cm_c_causa               ,
   cm_c_oficina             ,   
   cm_a_tipo                ,
   cm_i_movimiento          ,
   cm_c_moneda              ,      
   cm_i_iva_basico          , 
   cm_i_iva_percepcion      ,
   cm_i_iva_adicional       , 
   cm_d_concepto            , 
   cm_c_oficina_cta         ,
   cm_d_transaccion         , 
   cm_d_comprob_mov         ,
   cm_s_movimiento          ,
   cm_u_usuario             ,
   cm_m_estado              ,
   cm_m_tipo_movim          ,
   cm_s_movimiento_fv       )
   select
   tm_fecha,
   tm_fecha_fv,
   tm_hora,
   tm_tipo_tran,         
   isnull((case when convert (int,tm_causa)>= 5000 
              then convert (varchar(5),convert(int, tm_causa)-5000)
              else tm_causa
         end),' '),
   tm_oficina  ,
   (case when tm_signo = 'C' then '+' 
                             else '-' 
        end),
   (case when tm_iva_basico > 0 or tm_iva_percepcion > 0 or tm_iva_adicional>0 
         then tm_valor_base 
         else tm_valor
         end),          
   tm_moneda,
   tm_iva_basico,
   tm_iva_percepcion,
   tm_iva_adicional,
   tm_concepto,            
   tm_oficina_cta,
   null,
   convert(varchar(30),isnull(isnull(tm_boleta,tm_cheque),tm_secuencial)),
   isnull(tm_secuencial_fv,tm_cod_alterno),
   tm_usuario,
   (case when tm_estado != 'R' or tm_estado is null then 'N' else 'S' end ),
   'DD',
   tm_secuencial_fv
   from  cob_cuentas..cc_tran_monet            
   where tm_cta_banco           = @w_n_cta_banco_cobis
   and   tm_fecha_fv           >= @i_f_desde_movim
   and   tm_fecha_fv           <= @i_f_hasta_movim  
   and   tm_valor               > 0
   and   isnull(tm_indicador,0) < 2
   and   ( ( @i_m_ver_reversados = 'N' and (tm_estado   <> 'R' or tm_estado   is null) ) 
           or 
           ( @i_m_ver_reversados = 'S' ) )   
   order by tm_cta_banco, tm_fecha_fv, tm_secuencial_fv, tm_fecha, tm_cod_alterno asc

   select 
   @w_k_registros = @w_k_registros + @@rowcount,
   @w_n_error     = @@error

   if @w_n_error <> 0
   begin --CONTROL DE ERROR
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_mov (1)'
      goto error_trap                             
   
   end   --CONTROL DE ERROR
   
    if @t_debug = 'S'
   begin
      print ' MOVIMIENTOS DIARIOS - ANTES DE IMPUESTOS'
      select *
      from  cob_cuentas..cc_tran_monet            
      where tm_cta_banco           = @w_n_cta_banco_cobis
      and   tm_fecha_fv           >= @i_f_desde_movim
      and   tm_fecha_fv           <= @i_f_hasta_movim  
      and   tm_valor               > 0
      and   isnull(tm_indicador,0) < 2
      and   ( ( @i_m_ver_reversados = 'N' and (tm_estado   <> 'R' or tm_estado   is null) ) 
              or 
              ( @i_m_ver_reversados = 'S' ) )   
      order by tm_cta_banco, tm_fecha_fv, tm_secuencial_fv, tm_fecha, tm_cod_alterno asc 
   end
   
  
end   -- OPERATIVO
-- ----------------------------------------------------------

-- ----------------------------------------------------------
-- MOVIMIENTOS HISTORICOS
-- ----------------------------------------------------------
if @w_m_movim_hist = 'S'   	
begin -- HISTORICO

   insert into #cc_t_consulta_mov(                  
   cm_f_movimiento          ,
   cm_f_movimiento_fv       ,
   cm_f_movimiento_hora     ,
   cm_c_tipo_tran           ,   
   cm_c_causa               ,
   cm_c_oficina             ,   
   cm_a_tipo                ,
   cm_i_movimiento          ,
   cm_c_moneda              ,      
   cm_i_iva_basico          , 
   cm_i_iva_percepcion      ,
   cm_i_iva_adicional       , 
   cm_d_concepto            , 
   cm_c_oficina_cta         ,
   cm_d_transaccion         , 
   cm_d_comprob_mov         ,
   cm_s_movimiento          ,
   cm_u_usuario             ,
   cm_m_estado              ,
   cm_m_tipo_movim          ,
   cm_s_movimiento_fv       )
   select
   hm_fecha,
   hm_fecha_fv,
   hm_hora,
   hm_tipo_tran,         
   isnull((case when convert (int,hm_causa)>= 5000 
              then convert (varchar(5),convert(int, hm_causa)-5000)
              else hm_causa
         end),' '),
   hm_oficina  ,
   (case when hm_signo = 'C' then '+' 
                             else '-' 
        end),
   (case when hm_iva_basico > 0 or hm_iva_percepcion > 0 or hm_iva_adicional>0         
         then hm_valor_base 
         else hm_valor
         end),          
   hm_moneda,
   hm_iva_basico,
   hm_iva_percepcion,
   hm_iva_adicional,
   hm_concepto,            
   hm_oficina_cta,
   null,
   convert(varchar(30),isnull(isnull(hm_boleta,hm_cheque),hm_secuencial)),
   isnull(hm_secuencial_fv,hm_cod_alterno),
   hm_usuario,
   (case when hm_estado != 'R' or hm_estado is null then 'N' else 'S' end ),
   'HH',
   hm_secuencial_fv
   from cob_cuentas_his..cc_his_movimiento (index cc_his_movimiento_2)      
   where hm_cta_banco           = @w_n_cta_banco_cobis
   and   hm_fecha_fv           >= @i_f_desde_movim   
   and   hm_fecha_fv           <= @i_f_hasta_movim
   and   hm_valor               > 0
   and   isnull(hm_indicador,0) < 2
   and   ( ( @i_m_ver_reversados = 'N' and (hm_estado   <> 'R' or hm_estado   is null) ) 
           or 
           ( @i_m_ver_reversados = 'S' ) )
   order by hm_cta_banco ,hm_fecha_fv, hm_secuencial_fv, hm_fecha, hm_cod_alterno asc

   select 
   @w_k_registros = @w_k_registros + @@rowcount,
   @w_n_error     = @@error

   if @w_n_error <> 0
   begin --CONTROL DE ERROR
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_mov (2)'
      goto error_trap                             
   
   end   --CONTROL DE ERROR            

end   -- HISTORICO
-- ----------------------------------------------------------
if @w_k_registros > 0 
begin -- SI HAY INFORMACION PARA MOSTRAR

    
   
   insert into #cc_t_consulta_tran( 
   ct_f_movimiento          ,
   ct_f_movimiento_fv       ,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran           ,   
   ct_c_causa               ,
   ct_c_oficina             ,   
   ct_a_tipo                ,
   ct_i_movimiento          ,
   ct_c_moneda              ,      
   ct_i_iva_basico          , 
   ct_i_iva_percepcion      ,
   ct_i_iva_adicional       , 
   ct_d_concepto            , 
   ct_c_oficina_cta         ,
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario             ,
   ct_m_estado              ,
   ct_m_tipo_movim          ,
   ct_s_movimiento_fv       )
   select 
   cm_f_movimiento          ,
   cm_f_movimiento_fv       ,
   cm_f_movimiento_hora     ,
   cm_c_tipo_tran           , 
   cm_c_causa               ,
   cm_c_oficina             , 
   cm_a_tipo                ,
   cm_i_movimiento          ,
   cm_c_moneda              , 
   cm_i_iva_basico          , 
   cm_i_iva_percepcion      ,
   cm_i_iva_adicional       , 
   cm_d_concepto            , 
   cm_c_oficina_cta         ,
   cm_d_transaccion         , 
   cm_d_comprob_mov         ,
   cm_s_movimiento          ,
   cm_u_usuario             ,
   cm_m_estado              ,
   cm_m_tipo_movim          ,
   cm_s_movimiento_fv       
   from  #cc_t_consulta_mov
   order by cm_f_movimiento_fv, cm_s_movimiento_fv, cm_f_movimiento, cm_s_movimiento 
   
   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_tran (1)'
      goto error_trap                             
      
   end   -- CONTROL DE ERROR
  
   -- --------------------------------------------------------------
   -- PASAJE DE TABLA DE TRABAJO DE 7X24 HACIA LA DE TRABAJO GENERAL
   -- --------------------------------------------------------------                  
   insert into #cc_t_consulta_tran(
   ct_f_movimiento           ,
   ct_f_movimiento_fv        ,
   ct_f_movimiento_hora      ,
   ct_c_tipo_tran            ,   
   ct_c_causa                ,
   ct_c_oficina              ,   
   ct_a_tipo                 ,
   ct_i_movimiento           ,
   ct_c_moneda               ,      
   ct_i_iva_basico           , 
   ct_i_iva_percepcion       ,
   ct_i_iva_adicional        , 
   ct_d_concepto             , 
   ct_c_oficina_cta          , 
   ct_d_transaccion          , 
   ct_d_comprob_mov          ,
   ct_s_movimiento           ,
   ct_u_usuario              ,
   ct_m_estado               ,
   ct_m_tipo_movim           ,
   ct_s_movimiento_fv        )
   select 
   cm7_f_movimiento          ,
   cm7_f_movimiento_fv       ,
   cm7_f_movimiento_hora     ,
   cm7_c_tipo_tran           ,   
   cm7_c_causa               ,
   cm7_c_oficina             ,   
   cm7_a_tipo                ,
   cm7_i_movimiento          ,
   cm7_c_moneda              ,      
   cm7_i_iva_basico          , 
   cm7_i_iva_percepcion      ,
   cm7_i_iva_adicional       , 
   cm7_d_concepto            , 
   cm7_c_oficina_cta         , 
   cm7_d_transaccion         , 
   cm7_d_comprob_mov         ,
   cm7_s_movimiento          ,
   isnull(cm7_u_usuario,'')  ,
   cm7_m_estado              ,
   cm7_m_tipo_movim          ,
   cm7_s_movimiento_fv       
   from #cc_t_consulta_mov7x24
   order by cm7_s_movimiento ASC       -- SECUENCIAL UNICO DE MOVIMIENTO
   
   select 
   @w_k_reg_link7x24 = @@rowcount,
   @w_n_error        = @@error
   
   if @w_n_error <> 0
   begin --CONTROL DE ERROR
       
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_tran (2)'
      goto error_trap                             
   
   end   --CONTROL DE ERROR      
   -- --------------------------------------------------------------
   -- --------------------------------------------------------------

   -- --------------------------------------------------------------------------   
   -- AGREGAR LA APERTURA DE MOVIMIENTOS SOLO PARA AQUELLOS QUE TENGAN IMPUESTOS 
   -- --------------------------------------------------------------------------
   -- -----------------------------
   -- 1. IVA BASICO
   -- -----------------------------
   insert into #cc_t_consulta_tran( 
   ct_s_orden_orig           ,
   ct_f_movimiento          ,
   ct_f_movimiento_fv       ,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran           ,   
   ct_c_causa               ,
   ct_c_oficina             ,   
   ct_a_tipo                ,
   ct_i_movimiento          ,
   ct_c_moneda              ,      
   ct_i_iva_basico          ,
   ct_i_iva_percepcion      ,
   ct_i_iva_adicional       , 
   ct_d_concepto            , 
   ct_c_oficina_cta         , 
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario             ,
   ct_m_estado              ,
   ct_m_tipo_movim          ,
   ct_s_movimiento_fv )
   select
   ct_s_orden_consulta, 
   ct_f_movimiento,
   ct_f_movimiento_fv,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran,
   ct_c_causa,
   ct_c_oficina,
   ct_a_tipo,
   ct_i_iva_basico,
   ct_c_moneda,      
   0.00,
   0.00,
   0.00,
   'DEBITO FISCAL IVA BASICO',            
   ct_c_oficina_cta         , 
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario             ,
   ct_m_estado              ,
   substring(ct_m_tipo_movim,1,1)+'1',      -- CAMPO DE CONTROL PARA SIGUIENTE: ORIGEN DEL MOVIMIENTO -> 1=IVA R.I.
   ct_s_movimiento_fv 
   from #cc_t_consulta_tran
   where ct_i_iva_basico > 0        
   and   ct_c_tipo_tran <> 256
   order by ct_s_orden_consulta ASC
   
   select 
   @w_k_registros = @w_k_registros + @@rowcount,
   @w_n_error     = @@error

   if @w_n_error <> 0
   begin -- CONTROL DE ERROR       	 
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_tran (3)'
      goto error_trap                             
      
   end   -- CONTROL DE ERROR

   -- -----------------------------
   -- 2. IVA ADICIONAL
   -- -----------------------------
    insert into #cc_t_consulta_tran( 
   ct_s_orden_orig           ,
   ct_f_movimiento          ,
   ct_f_movimiento_fv       ,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran           ,   
   ct_c_causa               ,
   ct_c_oficina             ,   
   ct_a_tipo                ,
   ct_i_movimiento          ,
   ct_c_moneda              ,      
   ct_i_iva_basico          ,
   ct_i_iva_percepcion      ,
   ct_i_iva_adicional       , 
   ct_d_concepto            , 
   ct_c_oficina_cta         , 
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario             ,
   ct_m_estado              ,
   ct_m_tipo_movim          ,
   ct_s_movimiento_fv )
   select
   ct_s_orden_consulta, 
   ct_f_movimiento,
   ct_f_movimiento_fv,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran,
   ct_c_causa,
   ct_c_oficina,
   ct_a_tipo,
   ct_i_iva_adicional,  
   ct_c_moneda,      
   0.00,
   0.00,
   0.00,
   'IVA ADICIONAL RNI',  
   ct_c_oficina_cta, 
   ct_d_transaccion, 
   ct_d_comprob_mov,
   ct_s_movimiento,
   ct_u_usuario,
   ct_m_estado,
   substring(ct_m_tipo_movim,1,1)+'2',       -- CAMPO DE CONTROL PARA SIGUIENTE: ORIGEN DEL MOVIMIENTO -> 2=IVA R.N.I.
   ct_s_movimiento_fv
   from #cc_t_consulta_tran
   where ct_i_iva_adicional > 0        
   and   ct_c_tipo_tran    <> 256
   order by ct_s_orden_consulta ASC
      
   select 
   @w_k_registros = @w_k_registros + @@rowcount,
   @w_n_error     = @@error

   if @w_n_error <> 0
   begin -- CONTROL DE ERROR       	 
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_tran (4)'
      goto error_trap                             
      
   end   -- CONTROL DE ERROR

   -- -----------------------------
   -- 3. PERCEPCION
   -- -----------------------------
    insert into #cc_t_consulta_tran( 
   ct_s_orden_orig           ,
   ct_f_movimiento          ,
   ct_f_movimiento_fv       ,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran           ,   
   ct_c_causa               ,
   ct_c_oficina             ,   
   ct_a_tipo                ,
   ct_i_movimiento          ,
   ct_c_moneda              ,      
   ct_i_iva_basico          ,
   ct_i_iva_percepcion      ,
   ct_i_iva_adicional       , 
   ct_d_concepto            , 
   ct_c_oficina_cta         , 
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario             ,
   ct_m_estado              ,
   ct_m_tipo_movim          ,
   ct_s_movimiento_fv )
   select
   ct_s_orden_consulta, 
   ct_f_movimiento,
   ct_f_movimiento_fv,
   ct_f_movimiento_hora     ,
   ct_c_tipo_tran,
   ct_c_causa,
   ct_c_oficina,
   ct_a_tipo,
   ct_i_iva_percepcion, 
   ct_c_moneda,      
   0.00,
   0.00,
   0.00,
   'RETENCION IVA PERCEPCION',  
   ct_c_oficina_cta         , 
   ct_d_transaccion         , 
   ct_d_comprob_mov         ,
   ct_s_movimiento          ,
   ct_u_usuario,
   ct_m_estado,
   substring(ct_m_tipo_movim,1,1)+'3',       -- CAMPO DE CONTROL PARA SIGUIENTE: ORIGEN DEL MOVIMIENTO -> 3=PERCEPCION IVA
   ct_s_movimiento_fv     
   from #cc_t_consulta_tran
   where ct_i_iva_percepcion > 0        
   and   ct_c_tipo_tran     <> 256
   order by ct_s_orden_consulta ASC
      
   select 
   @w_k_registros = @w_k_registros + @@rowcount,
   @w_n_error     = @@error

   if @w_n_error <> 0
   begin -- CONTROL DE ERROR       	 
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_tran (5)'
      goto error_trap                             
      
   end   -- CONTROL DE ERROR

   -- ---------------------------------------------
   -- ACTUALIZACION MANUAL DEL SEGUNDO IDENTITY
   -- ---------------------------------------------
   update #cc_t_consulta_tran
   set ct_s_orden_orig = ct_s_orden_consulta
   where ct_s_orden_orig is null

   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
      select 
      @w_n_error   = 355028,    
      @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_tran'
      goto error_trap                             
   end   -- CONTROL DE ERROR
   -- ---------------------------------------------
   
   -- -------------------------------------------------------------
   -- ACTUALIZACION DEL CONCEPTO
   -- -------------------------------------------------------------
   update #cc_t_consulta_tran set 
   ct_d_transaccion = descripcion_causal 
   from cob_bcradgi..bc_trn_causales_pasivas
   where trn    = ct_c_tipo_tran 
   and   causal = ct_c_causa

   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
      select 
      @w_n_error   = 355028,    
      @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_tran (1)'
      goto error_trap                             
   end   -- CONTROL DE ERROR   
   
   update #cc_t_consulta_tran set 
   ct_d_transaccion = tn_descripcion 
   from cobis..cl_ttransaccion
   where tn_trn_code = ct_c_tipo_tran
   and   ct_d_transaccion is null

   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
      select 
      @w_n_error   = 355028,    
      @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_tran (2)'
      goto error_trap                             
   end   -- CONTROL DE ERROR

   update #cc_t_consulta_tran set 
   ct_d_transaccion = 'TRN: ' + convert(varchar(10), ct_c_tipo_tran) + ' - CAUSA: ' + ct_c_causa
   where ct_d_transaccion is null

   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
      select 
      @w_n_error   = 355028,    
      @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_tran (3)'
      goto error_trap                             
   end   -- CONTROL DE ERROR

   update #cc_t_consulta_tran 
   set ct_d_transaccion = left(ct_d_transaccion, 11) + ' ' + ct_d_concepto 
   where ct_d_concepto is not null

   if @@error <> 0
   begin -- CONTROL DE ERROR       	 
      select 
      @w_n_error   = 355028,    
      @w_d_mensaje = 'ERROR AL ACTUALIZAR TABLA DE TRABAJO #cc_t_consulta_tran (4)'
      goto error_trap                             
   end   -- CONTROL DE ERROR
   
     if @t_debug = 'S'
   begin
      print ' TABLA TEMPORAL DESPUES DE IMPUESTOS'
      select * from #cc_t_consulta_tran
   end
   
   -- -------------------------------------------------------------

   -- -----------------------------------------------------------------
   -- CREACION DE LA TABLA TEMPRORAL PARA ORDEN DESCENDENTE PARA EL FE
   -- -----------------------------------------------------------------
   create table #cc_t_consulta_mov_ord(
   cm2_s_orden_consulta      int         identity,
   cm2_f_movimiento          datetime    not null,
   cm2_f_movimiento_fv       datetime    not null,
   cm2_a_tipo                char(1)     not null,
   cm2_i_movimiento          money       not null,
   cm2_c_moneda              smallint    not null,    
   cm2_d_transaccion         varchar(60)     null,
   cm2_d_comprob_mov         varchar(45)     null,
   cm2_s_movimiento          int         not null,
   cm2_m_tipo_movim          char(2)         null,
   cm2_c_tipo_tran           int             null,
   cm2_c_causa               varchar(5)      null,
   cm2_c_oficina             smallint        null,
   cm2_m_estado              char(1)         null,
   cm2_u_usuario             varchar(30) not null,
   cm2_s_movimiento_fv       int             null) 
   
   if @@error <> 0
   begin --CONTROL DE ERROR
   
      select 
      @w_n_error   = 308028,    -- ERROR DE CREACION DE TABLA              
      @w_d_mensaje = 'ERROR AL CREAR LA TABLA DE TRABAJO #cc_t_consulta_mov_ord'
      goto error_trap                             
   
   end   --CONTROL DE ERROR
   
   create index cc_t_consulta_mov_ord_k01 on #cc_t_consulta_mov_ord( cm2_s_orden_consulta ASC )

   if @@error <> 0
   begin --CONTROL DE ERROR
   
      select 
      @w_n_error   = 308028,    -- ERROR DE CREACION DE TABLA              
      @w_d_mensaje = 'ERROR AL CREAR INDICE DE LA TABLA DE TRABAJO #cc_t_consulta_mov_ord'
      goto error_trap                             
   
   end   --CONTROL DE ERROR      
   
   insert into #cc_t_consulta_mov_ord(
   cm2_s_movimiento          ,
   cm2_f_movimiento          ,
   cm2_f_movimiento_fv       ,
   cm2_a_tipo                ,      
   cm2_c_moneda              ,   
   cm2_i_movimiento          ,                  
   cm2_d_transaccion         ,
   cm2_d_comprob_mov         ,
   cm2_m_tipo_movim          ,
   cm2_c_tipo_tran           ,
   cm2_c_causa               ,
   cm2_c_oficina             ,
   cm2_m_estado              ,
   cm2_u_usuario             ,
   cm2_s_movimiento_fv       )
   select 
   ct_s_movimiento          ,
   ct_f_movimiento_hora     , 
   ct_f_movimiento_fv       ,
   ct_a_tipo                ,      
   ct_c_moneda              ,   
   ct_i_movimiento          ,                  
   ct_d_transaccion         ,
   ct_d_comprob_mov         ,
   ct_m_tipo_movim          ,
   ct_c_tipo_tran           ,
   ct_c_causa               ,
   ct_c_oficina             ,
   ct_m_estado              ,
   ct_u_usuario             ,
   ct_s_movimiento_fv
   from #cc_t_consulta_tran
   order by ct_s_orden_orig DESC, ct_s_orden_consulta DESC

   if @@error <> 0
   begin -- CONTROL DE ERROR 
   
      select 
      @w_n_error   = 353021,    
      @w_d_mensaje = 'ERROR AL INSERTAR EN TABLA DE TRABAJO #cc_t_consulta_mov_ord'
      goto error_trap                             
      
   end   -- CONTROL DE ERROR
   
   
   
   -- --------------------------------------------------------    
   
   -- ------------------------------------------------------------
   -- UBICO EL ULT. REGISTRO VISUALIZADO PARA HACER EL SIGUIENTE
   -- ------------------------------------------------------------
   select @w_s_id_registro = 0
   
   if  @i_s_movim_hasta is not null
   and @i_f_movim_hasta is not null
   and @i_f_valor_hasta is not null
   and @i_s_valor_hasta is not null
   and @i_m_tipo_movim  is not null
   begin -- SI ESTOY EN UN SIGUIENTE
      
      select @w_s_id_registro     = cm2_s_orden_consulta 
      from   #cc_t_consulta_mov_ord 
      where  cm2_s_movimiento     = @i_s_movim_hasta
      and    cm2_f_movimiento     = @i_f_movim_hasta
      and    cm2_f_movimiento_fv  = @i_f_valor_hasta      
      and    cm2_s_movimiento_fv  = @i_s_valor_hasta
      and    cm2_m_tipo_movim     = @i_m_tipo_movim
	  
	  if @@rowcount = 0
	  begin
	     select 
         @w_n_error   = 353021,    
         @w_d_mensaje = 'VERIFIQUE QUE LOS DATOS PARA EL SIGUIENTE SEAN LOS CORRECTOS'
         goto error_trap  
	  end
      
   end   -- SI ESTOY EN UN SIGUIENTE
   
   if @t_debug = 'S'
   begin
      print ' 2168 - @w_s_id_registro <%1!> ',@w_s_id_registro
   end
   
   
   
   -- ------------------------------------------------------------

end   -- SI HAY INFORMACION PARA MOSTRAR
else
begin -- NO HAY INFORMACION PARA MOSTRAR - RESULTSET VACIO

   select
   'osMovimiento'      = " ",
   'ofMovimiento'      = " ",      
   'ofValor'           = " ",
   'osValor'           = " ",
   'ocTransaccionMovim'= " ",
   'ocCausalMovim'     = " ",
   'ocOficinaMovim'    = " ",
   'omDebitoCredito'   = " ",
   'oiMovimiento'      = " ",
   'ocMonedaMovim'     = " ",
   'ouMovimiento'      = " ",
   'odConceptoMov'     = " ",
   'odComprobMov'      = " ",
   'omMovimReversado'  = " ",
   'omTipoMovim'       = " "
   
   return 0

end   -- NO HAY INFORMACION PARA MOSTRAR - RESULTSET VACIO

-- ELIMINAR LA TABLA DE TRABAJO 
drop table #cc_t_consulta_mov

if @@error <> 0
begin -- CONTROL DE ERROR       	 

   select 
   @w_n_error   = 353021,    
   @w_d_mensaje = 'ERROR AL BORRAR EN TABLA DE TRABAJO #cc_t_consulta_mov'
   goto error_trap                             
   
end   -- CONTROL DE ERROR

-- ELIMINAR LA TABLA DE TRABAJO 
drop table #cc_t_consulta_mov7x24

if @@error <> 0
begin -- CONTROL DE ERROR 
   select 
   @w_n_error   = 207023,    
   @w_d_mensaje = 'ERROR AL BORRAR LA TABLA DE TRABAJO #cc_t_consulta_mov7x24'
   goto error_trap                                      
end   -- CONTROL DE ERROR

-- ELIMINAR LA TABLA DE TRABAJO 
drop table #cc_t_consulta_tran

if @@error <> 0
begin -- CONTROL DE ERROR 
   select 
   @w_n_error   = 207023,    
   @w_d_mensaje = 'ERROR AL BORRAR LA TABLA DE TRABAJO #cc_t_consulta_tran'
   goto error_trap                                      
end   -- CONTROL DE ERROR
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- SEPARACION DE LA INFORMACION SEGUN FILTROS DEL USUARIO - FIN
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------

-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- VISUALIZACION DE LA INFORMACION - INICIO
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
select 
@w_m_existe      = 'N',
@w_s_id_registro = isnull(@w_s_id_registro,0)

if exists (select 1                                 
           from #cc_t_consulta_mov_ord
           where cm2_s_orden_consulta > @w_s_id_registro )
begin -- HAY INFORMACION PARA LISTAR

   select @w_m_existe = 'S'
   
end   -- HAY INFORMACION PARA LISTAR

if @t_debug = 'S'
   begin
      print ' 2260 - @w_m_existe <%1!> ',@w_m_existe
   end



if @w_m_existe = 'S'
begin -- HAY INFORMACION
   
   set rowcount @i_k_filas
   
   select
   'osMovimiento'      = cm2_s_movimiento,         -- SECUENCIAL DE MOVIMIENTO
   'ofMovimiento'      = convert(varchar(10), cm2_f_movimiento, 101) + ' ' + substring( convert(varchar(8), cm2_f_movimiento, 8), 1, 5),
   'ofValor'           = convert(varchar(10), cm2_f_movimiento_fv, 101),   -- FECHA VALOR DE MOVIMIENTO
   'osValor'           = cm2_s_movimiento_fv,     -- SECUENCIAL DE MOVIMIENTO VALOR
   'ocTransaccionMovim'= cm2_c_tipo_tran,         -- TIPO DE TRANSACCION
   'ocCausalMovim'     = cm2_c_causa,             -- CAUSA
   'ocOficinaMovim'    = cm2_c_oficina,            -- OFICINA NUMERO
   'omDebitoCredito'   = cm2_a_tipo,               -- CREDITO O DEBITO 
   'oiMovimiento'      = cm2_i_movimiento,         -- MONTO
   'ocMonedaMovim'     = cm2_c_moneda,             -- MONEDA DE MOVIMIENTO 
   'ouMovimiento'      = cm2_u_usuario,            -- USUARIO QUE HIZO EL MOVIMIENTO
   'odConceptoMov'     = cm2_d_transaccion,        -- CONCEPTO
   'odComprobMov'      = cm2_d_comprob_mov ,       -- COMPROBANTE
   'omMovimReversado'  = cm2_m_estado,             -- ESTADO REVERSADO S-SI O N-NO
   'omTipoMovim'       = cm2_m_tipo_movim          -- TIPO DE MOVIMIENTO PARA EL SIGUIENTE
   from #cc_t_consulta_mov_ord
   where cm2_s_orden_consulta > @w_s_id_registro  
   order by cm2_s_orden_consulta asc
   
   set rowcount 0
   
   if @t_debug = 'S'
   begin      
      select '2295-RESULSET FINAL',* 
	  from #cc_t_consulta_mov_ord
      order by cm2_s_orden_consulta asc
   end
   
end   -- HAY INFORMACION
else
begin -- RESULTSET VACIO

   select
   'osMovimiento'      = " ",
   'ofMovimiento'      = " ",      
   'ofValor'           = " ",
   'osValor'           = " ",
   'ocTransaccionMovim'= " ",
   'ocCausalMovim'     = " ",
   'ocOficinaMovim'    = " ",
   'omDebitoCredito'   = " ",
   'oiMovimiento'      = " ",
   'ocMonedaMovim'     = " ",
   'ouMovimiento'      = " ",
   'odConceptoMov'     = " ",
   'odComprobMov'      = " ",
   'omMovimReversado'  = " ",
   'omTipoMovim'       = " "
   
   return 0
 
end   -- RESULTSET VACIO
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------
-- VISUALIZACION DE LA INFORMACION - FIN
-- ---------------------------------------------------------------
-- ---------------------------------------------------------------

return 0

error_trap:

select @w_d_mensaje = isnull(@w_d_mensaje, mensaje),
       @w_sev       = isnull(@w_sev, severidad)
from  cobis..cl_errores
where numero = @w_n_error

select @w_d_mensaje = isnull(@w_d_mensaje,'NO EXISTE MENSAJE ASOCIADO')

select @w_d_mensaje = '[' + @w_sp_name + ']   ' + upper(@w_d_mensaje)

if @i_quien_llama = 'F' 
begin -- EL SP FUE LLAMADO DESDE EL FRONT-END ===> NECESITO QUE SAQUE EL MENSAJE POR PANTALLA
   exec cobis..sp_cerror
   @t_from = @w_sp_name,
   @i_num  = @w_n_error,
   @i_sev  = 1,
   @i_msg  = @w_d_mensaje
end   -- EL SP FUE LLAMADO DESDE EL FRONT-END ===> NECESITO QUE SAQUE EL MENSAJE POR PANTALLA

return @w_n_error


/*<returns>
<return value = "@w_n_error" description="VARIABLE GENERICA/DEVOLUCION SP" />
<return value = "0" description="EJECUCION EXITOSA" />
<return value = "201004" description="CUENTA NO EXISTE" /> 
<return value = "2630201" description="DEBE INGRESAR LAS FECHAS DESDE Y HASTA PARA REALIZAR LA CONSULTA" />  
<return value = "201065" description="FECHA DESDE MAYOR A FECHA HASTA" />
<return value = "308028" description="ERROR DE CREACION DE TABLA" /> 
<return value = "2900124" description="ERROR EN LA CREACION DEL INDICE PARA LA TABLA TEMPORAL" />
<return value = "201502" description="TIPO DE CUENTA OBLIGATORIO" />
<return value = "201501" description="TIPO DE CUENTA INVALIDO" />
<return value = "701302" description="NUMERO DE DIAS SUPERA EL PARAMETRO" />
<return value = "353021" description="ERROR AL INSERTAR REGISTRO" />
<return value = "355028" description="ERROR AL ACTUALIZAR REGISTRO " />
<return value = "357006" description="ERROR AL ELIMINAR REGISTRO " />
<return value = "207023" description="ERROR AL ELIMINAR TABLA" />
</returns>*/
 
--<keyword>sp_listar_movim_cuenta</keyword>
--<keyword>CONSULTA DE MOVIMIENTOS DE CUENTA CORRIENTE PARA UN RANGO DE FECHA Y VISUALIZACION DE MOV. REVERSADOS</keyword>

/*<dependency ObjName="" xtype="" dependentObjectName="" dependentObjectType="" />*/
go