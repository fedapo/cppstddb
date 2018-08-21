#ifndef CPPSTDDB_DATABASE_ORACLE_H
#define CPPSTDDB_DATABASE_ORACLE_H

#include "oci_helper.h"
#include <cppstddb/front.h>
#include <cppstddb/util.h>
#include <oci.h>
#include <vector>
#include <sstream>
#include <cstring>

namespace cppstddb { namespace oracle {

    namespace impl {

        template<class P> class database;
        template<class P> class connection;
        template<class P> class statement;
        template<class P> class rowset;
        template<class P> struct bind_type;
        template<class P,class T> struct field;

        template<class P> using cell_t = cppstddb::front::cell<database<P>>;

        bool is_error(int status) {
            return 
                status != OCI_SUCCESS &&
                status != OCI_SUCCESS_WITH_INFO;
        }

        template<class S> void raise_error(const S& msg) {
            throw database_error(msg);
        }

        template<class S> void raise_error(const S& msg, int ret) {
            throw database_error(msg, ret);
        }

        /*
           template<class S> void raise_error(const S& msg, MYSQL_STMT* stmt, int ret) {
           throw database_error(msg, ret, mysql_stmt_error(stmt));
           }
         */

        template<class S> void check(const S& msg, sword status) {
            DB_TRACE(msg << ":" << status);
            if (is_error(status)) raise_error(msg,status);
        }

        template<class S, class T> T* check(const S& msg, T* ptr) {
            DB_TRACE(msg << ": " << static_cast<void*>(ptr));
            if (!ptr) raise_error(msg);
            return ptr;
        }

        template<class S> void check(const S& msg, sword status, OCIError* err_hndl) {
          DB_TRACE(msg << ":" << status);
          text errbuf[512];
          sb4 errcode = 0;
          OCIErrorGet((void*)err_hndl, 1, nullptr, &errcode,
                      errbuf, sizeof(errbuf), OCI_HTYPE_ERROR);
          if(is_error(status))
            raise_error(std::string((char*)errbuf), errcode);
        }

        template<class P> class database {
            public:
                using policy_type = P;
                using string = typename policy_type::string;
                using connection = connection<policy_type>;
                using statement = statement<policy_type>;
                using rowset = rowset<policy_type>;
                using bind_type = bind_type<policy_type>;
                template<typename T> using field_type = field<policy_type,T>;

                OCIEnv* env_hndl_;
                OCIError* err_hndl_;

                database() {
                    DB_TRACE("oracle: opening database");

                    // allocate OCI environment
                    auto st = OCIEnvCreate(&env_hndl_, OCI_THREADED | OCI_OBJECT,
                                           nullptr, nullptr, nullptr, nullptr,
                                           0, nullptr);
                    check("OCIEnvCreate", st);

                    st = OCIHandleAlloc(env_hndl_, (void**)&err_hndl_, OCI_HTYPE_ERROR, 0, nullptr);
                    check("OCIHandleAlloc", st);
                }

                ~database() {
                    auto st = OCIHandleFree(env_hndl_, OCI_HTYPE_ENV);
                    st = OCIHandleFree(err_hndl_, OCI_HTYPE_ERROR);
                }

                string date_column_type() const {return "date";}
        };

        template<class P> class connection {
            public:
                using policy_type = P;
                using database = database<policy_type>;

                OCIServer* srvhp_;
                OCISvcCtx* svc_ctx;
                OCISession* authp_;
                //OCIHandle<OCIError, OCI_HTYPE_ERROR> err_hndl_;

            public:
                database& db;

                connection(database& db_, const source& src)
                  : db(db_)
                  //, err_hndl_(db.env_hndl_)
                {
                    DB_TRACE("connection");

                    // allocate server and service context handles
                    auto st = OCIHandleAlloc(db.env_hndl_, (void**)&srvhp_, OCI_HTYPE_SERVER, 0, nullptr);
                    check("OCIHandleAlloc(OCI_HTYPE_SERVER)", st);

                    st = OCIHandleAlloc(db.env_hndl_, (void**)&svc_ctx, OCI_HTYPE_SVCCTX, 0, nullptr);
                    check("OCIHandleAlloc(OCI_HTYPE_SVCCTX)", st);

                    std::ostringstream ss;
                    ss << src.server << ":" << src.port << "/" << src.database;

                    // attach to the server
                    st = OCIServerAttach(srvhp_, db.err_hndl_,
                                         (text*)ss.str().c_str(), (sb4)ss.str().length(), 0);
                    check("OCIServerAttach", st, db.err_hndl_);

                    // set server in the service context
                    st = OCIAttrSet(svc_ctx, OCI_HTYPE_SVCCTX, (dvoid*)srvhp_, 0, OCI_ATTR_SERVER, db.err_hndl_);

                    // allocate session handle
                    st = OCIHandleAlloc(db.env_hndl_, (void**)&authp_, OCI_HTYPE_SESSION, 0, nullptr);

                    // set user name and password
                    st = OCIAttrSet(authp_, OCI_HTYPE_SESSION,
                                    (void*)src.username.c_str(), (ub4)src.username.length(),
                                    OCI_ATTR_USERNAME, db.err_hndl_);
                    st = OCIAttrSet(authp_, OCI_HTYPE_SESSION,
                                    (void*)src.password.c_str(), (ub4)src.password.length(),
                                    OCI_ATTR_PASSWORD, db.err_hndl_);

                    // connect
                    st = OCISessionBegin(svc_ctx, db.err_hndl_, authp_, OCI_CRED_RDBMS, OCI_DEFAULT);
                    check("OCISessionBegin", st, db.err_hndl_);

                    // set session in the service context
                    st = OCIAttrSet(svc_ctx, OCI_HTYPE_SVCCTX, authp_, 0, OCI_ATTR_SESSION, db.err_hndl_);
                }

                ~connection() {
                    DB_TRACE("~connection");

                    auto st = OCISessionEnd(svc_ctx, db.err_hndl_, authp_, OCI_DEFAULT);
                    check("OCISessionEnd", st, db.err_hndl_);

                    st = OCIServerDetach(srvhp_, db.err_hndl_, OCI_DEFAULT);
                    check("OCIServerDetach", st, db.err_hndl_);
                }

                OCISvcCtx* handle()
                {
                  return svc_ctx;
                }

                void commit() {
                  DB_TRACE("commit");
                  auto st = OCITransCommit(svc_ctx, db.err_hndl_, OCI_DEFAULT);
                  check("OCITransCommit", st, db.err_hndl_);
                }

                void rollback()
                {
                  DB_TRACE("rollback");
                  auto st = OCITransRollback(svc_ctx, db.err_hndl_, OCI_DEFAULT);
                  check("OCITransRollback", st, db.err_hndl_);
                }
        };

        template<class P> class statement {
            public:
                using policy_type = P;
                using string = typename policy_type::string;
                using connection = connection<policy_type>;
                using rowset = rowset<policy_type>;
                //MYSQL_STMT* stmt;
                OCIHandle<OCIStmt, OCI_HTYPE_STMT> stmt_hndl_;
                //OCIHandle<OCIError, OCI_HTYPE_ERROR> err_hndl_;
                string sql;
                int binds;
                connection& con_;

            public:
                statement(connection& con, const string& sql_)
                  : con_(con)
                  , sql(sql_)
                  , binds(0)
                  , stmt_hndl_(con.db.env_hndl_)
                  //, err_hndl_(con.db.env_hndl_)
                {
                    DB_TRACE("stmt: " << sql);
                    //stmt = check("mysql_stmt_init", mysql_stmt_init(con.mysql));

                    // set prefetch count
                    int prefetch_rows = 100;
                    auto st = OCIAttrSet(stmt_hndl_.get(), OCI_HTYPE_STMT, (void*)&prefetch_rows, sizeof(int),
                                         OCI_ATTR_PREFETCH_ROWS, con_.db.err_hndl_); //errhp_.get());
                }

                ~statement() {
                    DB_TRACE("~stmt");
                    //if (stmt) mysql_stmt_close(stmt);
                }

                OCIStmt* handle()
                {
                  return stmt_hndl_.get();
                }

                void prepare() {
                    DB_TRACE("prepare sql: " << sql);
                    auto st = OCIStmtPrepare(stmt_hndl_.get(), con_.db.err_hndl_, //errhp_.get(),
                                             (text*)sql.c_str(), (ub4)sql.length(),
                                             OCI_NTV_SYNTAX, OCI_DEFAULT);
                    check("OCIStmtPrepare", st, con_.db.err_hndl_);
                }

                void get_select_columns()
                {
                    // determine what type of SQL statement was prepared
                    ub2 stmt_type = OCI_STMT_UNKNOWN;
                    auto st = OCIAttrGet(stmt_hndl_.get(), OCI_HTYPE_STMT, &stmt_type,
                                         nullptr, OCI_ATTR_STMT_TYPE, con_.db.err_hndl_);

                    if(stmt_type == OCI_STMT_SELECT)
                    {
                      // execute the statement just to get a description of it
                      st = OCIStmtExecute(con_.svc_ctx, stmt_hndl_.get(), con_.db.err_hndl_,
                                          0, 0, nullptr, nullptr, OCI_DESCRIBE_ONLY);
                      check("OCIStmtExecute", st, con_.db.err_hndl_);

                      // get the number of columns in the select list
                      ub4 parmcnt;
                      st = OCIAttrGet(stmt_hndl_.get(), OCI_HTYPE_STMT, &parmcnt,
                                      nullptr, OCI_ATTR_PARAM_COUNT, con_.db.err_hndl_);

                      binds = parmcnt;

                      // go through the column list and retrieve the datatype of each column
                      for(ub4 i = 0; i < parmcnt; ++i)
                      {
                        /*describe_type t;
                        t.index = i + 1;

                        // get parameter for column i
                        st = OCIParamGet(stmt_hndl_.get(), OCI_HTYPE_STMT,
                                         con_.db.err_hndl_, (void**)&t.field, i + 1); // NOTE: column index is 1-based

                        // get data-type of column i
                        ub2 type = 0;
                        st = OCIAttrGet(colhd, OCI_DTYPE_PARAM, &type, nullptr, OCI_ATTR_DATA_TYPE, con_.db.err_hndl_);

                        ub4 colnamlen;
                        text* colname;
                        st = OCIAttrGet(colhd, OCI_DTYPE_PARAM, &colname, &colnamlen, OCI_ATTR_NAME, con_.db.err_hndl_);

                        t.name = colname;

                        DB_TRACE("Column " << (i + 1) << ":" << "(" << type << ") " << colname);

                        // shoule we free the memory of this column handle?
                        //OCIDescriptorFree(colhd, OCI_DTYPE_PARAM);*/
                      }
                    }
                }

                statement& query() {
                    // execute the statement and perform the initial fetch of rows into the defined array
                    const int nrows = 1;
                    auto st = OCIStmtExecute(con_.svc_ctx, stmt_hndl_.get(), con_.db.err_hndl_, //errhp_.get(),
                                             nrows, 0, nullptr, nullptr, OCI_DEFAULT);
                    check("OCIStmtExecute", st, con_.db.err_hndl_);
                    //check("mysql_stmt_execute", stmt, mysql_stmt_execute(stmt));
                    return *this;
                }

                template <typename... Args> statement& query(Args... args)
                {
                    //info("HERE: ",args...);
                    return *this;
                }
        };

        template<class P> struct describe_type {
            using policy_type = P;
            using string = typename policy_type::string;
            int index;
            string name;
            //MYSQL_FIELD* field;
            OCIParam* field; // column handle
        };

        template<class P> struct bind_type {
            value_type type;
            int mysql_type;
            int alloc_size;
            void* data;
            unsigned long length; // check type
            //my_bool is_null;
            //my_bool error;
        };

        template<class P> struct bind_context {
            using bind_type = bind_type<P>;
            using describe_type = describe_type<P>;
            bind_context(const describe_type& describe_, bind_type& bind_):
                row_array_size(1),describe(describe_),bind(bind_) {}
            int row_array_size;
            const describe_type& describe;
            bind_type& bind;
        };

        template<class P> struct bind_info {
            using bind_context = bind_context<P>;
            static const bind_info info[];
            int mysql_type;
            void (*bind)(bind_context& ctx);
        };

        template<class P> void binder(bind_context<P>& ctx) {
            /*auto type = ctx.describe.field->type;           
              const bind_info<P>* info = &bind_info<P>::info[0];
              for (auto i = &info[0]; i->mysql_type; ++i){
                  if (i->mysql_type == type) {
                  i->bind(ctx);
                  return;
              }
            }
            */

            /*
            std::stringstream s;
            s << "binder: type not found: " << ctx.describe.field->type;
            throw database_error(s.str());
            */

            //DB_WARN("type not found, binding to string for now: " << type);
            //bind_string(ctx);
        }

        template<class P> void bind_long(bind_context<P>& ctx) {
            ctx.bind.mysql_type = ctx.describe.field->type;
            ctx.bind.type = value_int;
            ctx.bind.alloc_size = ctx.describe.field->length; // ???
        }

        template<class P> void bind_date(bind_context<P>& ctx) {
            ctx.bind.mysql_type = ctx.describe.field->type;
            ctx.bind.type = value_date;
            ctx.bind.alloc_size = sizeof(OCIDateTime); // sizeof(OCIDate);
            //ctx.bind.alloc_size = sizeof(MYSQL_TIME);
        }

        template<class P> void bind_string(bind_context<P>& ctx) {
            ctx.bind.mysql_type = ctx.describe.field->type;
            ctx.bind.type = value_string;
            //ctx.bind.alloc_size = ctx.describe.field->length + 1;
        }

        template<class P> const bind_info<P> bind_info<P>::info[] = {
            /*
            { MYSQL_TYPE_TINY, bind_long<P> },
            { MYSQL_TYPE_SHORT, bind_long<P> },
            { MYSQL_TYPE_LONG, bind_long<P> },
            { MYSQL_TYPE_LONGLONG, bind_long<P> },
            { MYSQL_TYPE_DATE, bind_date<P> },
            // MYSQL_TYPE_DATETIME
            { MYSQL_TYPE_STRING, bind_string<P> },
            { 0, nullptr }
             */
        };

        template<class P> class rowset {
            public:
                using policy_type = P;
                using cell_t = cell_t<policy_type>;
                using statement = statement<policy_type>;
                using bind_type = bind_type<policy_type>;
                using bind_context = bind_context<policy_type>;
                statement& stmt_;
                //Allocator* allocator;
                unsigned int columns;

                //MYSQL_RES* result_metadata;
                int status;

                using describe_type = describe_type<policy_type>;
                using describe_vector = std::vector<describe_type>;
                using bind_vector = std::vector<bind_type>;
                //using mysql_bind_vector = std::vector<OCIBind*>;
                using mysql_bind_vector = std::vector<OCIDefine*>;
                //using mysql_bind_vector = std::vector<MYSQL_BIND>;
                //using mysql_bind_vector = std::vector<int>;

                describe_vector describes;
                bind_vector binds;
                mysql_bind_vector mysql_binds;

                //static const maxData = 256;

            public:
                rowset(statement& stmt, int rowArraySize_)
                    : stmt_(stmt) {
                        //allocator = stmt.allocator;

                        //result_metadata = check("mysql_stmt_result_metadata",
                        //                        mysql_stmt_result_metadata(stmt.stmt));

                        //if (!result_metadata) return; // check this
                        //columns = mysql_num_fields(result_metadata);
                        DB_TRACE("columns: " << columns);

                        build_describe();
                        build_bind();
                    }

                ~rowset() {
                    DB_TRACE("~rowset");

                    //foreach(b; bind) allocator.deallocate(b.data);

                    for(auto&& b : binds) {
                        //DB_TRACE("free: " << ", data: " << b.data << ", size: " << b.alloc_size);
                        free(b.data);
                    }

                    /*if (result_metadata) {
                        check("mysql_free_result");
                        mysql_free_result(result_metadata);
                    }*/
                }

                void build_describe() {
                    //columns = mysql_stmt_field_count(stmt.stmt);

                    describes.reserve(columns);

                    for(int i = 0; i != columns; ++i) {
                        auto& d = describes.emplace_back();

                        // get parameter for this column
                        sword st = OCIParamGet(stmt_.stmt_hndl_.get(), OCI_HTYPE_STMT,
                                               stmt_.con_.db.err_hndl_, (void**)&d.field, i + 1);
                        //check("OCIParamGet - OCI_HTYPE_STMT", st);

                        // get data-type of this column
                        //st = OCIAttrGet(d.field, OCI_DTYPE_PARAM,
                        //                &dbtext->type_, NULL, OCI_ATTR_DATA_TYPE, stmt_.con_.err_hndl_);

                        ub4 colnamelen;
                        text* colname;
                        st = OCIAttrGet(d.field, OCI_DTYPE_PARAM,
                                        (void**)&colname, (ub4*)&colnamelen,
                                        OCI_ATTR_NAME, stmt_.con_.db.err_hndl_);
                        //check("OCIAttrGet - OCI_DTYPE_PARAM - OCI_ATTR_NAME", st);

                        d.index = i;
                        d.name = (char*)colname;
                        //d.field = check("mysql_fetch_field", mysql_fetch_field(result_metadata));
                        //d.name = d.field->name;

                        //DB_TRACE("describe: name: ", d.name, ", mysql type: ", d.field.type);
                        DB_TRACE("describe: name: " << d.name);
                    }
                }

                void build_bind() {
                    binds.reserve(columns);

                    for(int i = 0; i != columns; ++i) {
                        auto& d = describes[i];
                        auto& b = binds.emplace_back();

                        bind_context ctx(d, b);
                        binder(ctx);

                        //b.data = allocator.allocate(b.alloc_size);

                        b.data = malloc(b.alloc_size);
                        //DB_TRACE("malloc: " << i << ", data: " << b.data << ", size: " << b.alloc_size);
                    }

                    setup(binds, mysql_binds);
                    //my_bool result = mysql_stmt_bind_result(stmt.stmt, &mysql_binds[0]);
                }

                static void setup(bind_vector& binds, mysql_bind_vector& mysql_binds) {
                    /*mysql_binds.assign(binds.size(), nullptr);
                    for(int i = 0; i != binds.size(); ++i) {
                        // FED ???? bind or define?
                        auto st = OCIDefineByPos(stmt_.stmt_hndl_.get(), &defnpp, stmt_.con_.db.err_hndl_, 1, (void*)field1, field_width,
                                                 SQLT_STR, (void*)field1_ind,
                                                 field1_len, field1_code, OCI_DEFAULT);
                    }
                    // make this efficient
                    mysql_binds.assign(binds.size(), MYSQL_BIND());
                    for(int i = 0; i != binds.size(); ++i) {
                        auto& b = binds[i];
                        auto& mb = mysql_binds[i];
                        memset(&mb, 0, sizeof(MYSQL_BIND)); //header?
                        mb.buffer_type = static_cast<enum_field_types>(b.mysql_type); // fix
                        mb.buffer = b.data;
                        mb.buffer_length = b.alloc_size;
                        mb.length = &b.length;
                        mb.is_null = &b.is_null;
                        mb.error = &b.error;
                    }*/
                }

                //bool hasResult() {return result_metadata != null;}

                int fetch() {
                    return next();
                }

                int next() {
                    const int nrows = 100;
                    // fetch another set of rows
                    sword st = OCIStmtFetch2(stmt_.stmt_hndl_.get(), stmt_.con_.db.err_hndl_, nrows, OCI_FETCH_NEXT, 0, OCI_DEFAULT);
                    check("OCIStmtFetch2", st, stmt_.con_.db.err_hndl_);
                    if(st == OCI_SUCCESS || st == OCI_SUCCESS_WITH_INFO) {
                      return 1;
                    }
                    else if(st == OCI_NO_DATA) {
                      //rows_ = row_count_;
                      return 0;
                    }
                    else {
                      text errbuf[512];
                      sb4 errcode = 0;
                      OCIErrorGet((void*)stmt_.con_.db.err_hndl_, 1, nullptr, &errcode,
                                  errbuf, sizeof(errbuf), OCI_HTYPE_ERROR);
                      raise_error(std::string((char*)errbuf), errcode);
                    }

                    /*status = check("mysql_stmt_fetch", stmt.stmt, mysql_stmt_fetch(stmt.stmt));
                    if (!status) {
                        return 1;
                    } else if (status == MYSQL_NO_DATA) {
                        //rows_ = row_count_;
                        return 0;
                    } else if (status == MYSQL_DATA_TRUNCATED) {
                        raise_error("mysql_stmt_fetch: truncation", status);
                    }
                    raise_error("mysql_stmt_fetch", stmt.stmt, status);*/
                    return 0;
                }

                auto name(size_t idx) {
                    return describes[idx].name;
                }

        };


        template<class P, typename T> struct field {};

        template<class P> struct field<P,std::string> {
            static std::string as(const rowset<P>& r, const cell_t<P>& cell) {
                return static_cast<const char *>(cell.bind_.data);
            }
        };

        template<class P> struct field<P,int> {
            static int as(const rowset<P>& r, const cell_t<P>& cell) {
                return *static_cast<int*>(cell.bind_.data);
            }
        };

        template<class P> struct field<P,date_t> {
            static date_t as(const rowset<P>& r, const cell_t<P>& cell) {
                //auto& t = *static_cast<MYSQL_TIME*>(cell.bind_.data);
                //return date_t(t.year, t.month, t.day);
                return date_t(0,0,0);
            }
        };

    }

    using database = cppstddb::front::basic_database<impl::database<default_policy>>;

    inline auto create_database() {
        return database();
    }


}}

#endif


