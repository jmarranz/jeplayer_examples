package example.jeplayer.jooq.dao;

import example.jeplayer.jooq.model.Contact;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import jepl.JEPLColumnDesc;
import jepl.JEPLConnection;
import jepl.JEPLDAO;
import jepl.JEPLNonJTADataSource;
import jepl.JEPLPersistAction;
import jepl.JEPLPreparedStatement;
import jepl.JEPLPreparedStatementListener;
import jepl.JEPLResultSet;
import jepl.JEPLResultSetDALListener;
import jepl.JEPLResultSetDAO;
import jepl.JEPLResultSetDAOBeanMapper;
import jepl.JEPLResultSetDAOListener;
import jepl.JEPLTask;
import jepl.JEPLUpdateDAOBeanMapper;
import jepl.JEPLUpdateDAOListener;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.jooq.impl.DefaultDSLContext;

/**
 *  https://groups.google.com/forum/#!topic/jooq-user/2HTS-0DE1M8
 * 
 *  Because we're using JOOQ, binding params by name (provided by JEPLayer) are not used in this example
 * 
 * @author jmarranz
 */
public class ContactDAO
{
    protected JEPLDAO<Contact> dao;
    protected DefaultDSLContext jooqCtx;
    protected JEPLUpdateDAOListener<Contact> updateListener;    
    protected JEPLResultSetDAOListener<Contact> resultSetListener; 
    
    public ContactDAO(JEPLNonJTADataSource jds,DefaultDSLContext jooqCtx)
    {
        this(jds,jooqCtx,(short)0);        
    }
    
    public ContactDAO(JEPLNonJTADataSource jds,DefaultDSLContext jooqCtx,short mappingMode)
    {
        this.dao = jds.createJEPLDAO(Contact.class); 
        this.jooqCtx = jooqCtx;
        
        // This 3 mapping approaches provides the same behaviour in this simple example, they are coded 
        // just to show the different options for mapping         
        switch(mappingMode)
        {
            case 0: // default mapping attribs and columns by name ignoring case
                this.updateListener = jds.createJEPLUpdateDAOListenerDefault(Contact.class);
                this.resultSetListener = jds.createJEPLResultSetDAOListenerDefault(Contact.class); 
                break;
            case 1: // custom mapping 
                this.updateListener = new JEPLUpdateDAOListener<Contact>() {
                        @Override
                        public String getTable(JEPLConnection jcon, Contact obj) throws Exception {
                            return "CONTACT";
                        }

                        @Override
                        public Map.Entry<JEPLColumnDesc, Object>[] getColumnDescAndValues(JEPLConnection jcon, Contact obj, JEPLPersistAction action) throws Exception 
                        {
                            Map.Entry<JEPLColumnDesc,Object>[] result = new AbstractMap.SimpleEntry[]
                            {
                                new AbstractMap.SimpleEntry<>(new JEPLColumnDesc("ID").setAutoIncrement(true).setPrimaryKey(true),obj.getId()),
                                new AbstractMap.SimpleEntry<>(new JEPLColumnDesc("NAME"),obj.getName()),                    
                                new AbstractMap.SimpleEntry<>(new JEPLColumnDesc("PHONE"),obj.getPhone()),                    
                                new AbstractMap.SimpleEntry<>(new JEPLColumnDesc("EMAIL"),obj.getEmail())                    
                            };
                            return result;
                        }            
                    };                
                
                this.resultSetListener = new JEPLResultSetDAOListener<Contact>() {
                        @Override
                        public void setupJEPLResultSet(JEPLResultSet jrs,JEPLTask<?> task) throws Exception {
                        }

                        @Override
                        public Contact createObject(JEPLResultSet jrs) throws Exception {
                            return new Contact();
                        }

                        @Override
                        public void fillObject(Contact obj,JEPLResultSet jrs) throws Exception {
                            ResultSet rs = jrs.getResultSet();
                            obj.setId(rs.getInt("ID"));
                            obj.setName(rs.getString("NAME"));
                            obj.setPhone(rs.getString("PHONE"));
                            obj.setEmail(rs.getString("EMAIL"));
                        }    
                    };
                break;
            case 2:  // default mapping using custom row-mappers              
                JEPLUpdateDAOBeanMapper<Contact> updateMapper = (Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action) -> { // public Object getColumnFromBean(Contact obj, JEPLConnection jcon, String columnName, Method getter, JEPLPersistAction action) throws Exception
                        if (columnName.equalsIgnoreCase("email"))
                        {
                            return obj.getEmail();
                        }
                        return JEPLUpdateDAOBeanMapper.NO_VALUE;
                    };                                    
                this.updateListener = jds.createJEPLUpdateDAOListenerDefault(Contact.class,updateMapper);
                    
                JEPLResultSetDAOBeanMapper<Contact> resultMapper = (Contact obj, JEPLResultSet jrs, int col, String columnName, Object value, Method setter) -> { // setColumnInBean(...)
                        if (columnName.equalsIgnoreCase("email"))
                        {
                            obj.setEmail((String)value);
                            return true;
                        }
                        return false;
                    };
                this.resultSetListener = jds.createJEPLResultSetDAOListenerDefault(Contact.class,resultMapper);                
                break;
            case 3:
                throw new RuntimeException("Unexpected");
        }
          
        dao.addJEPLListener(updateListener);        
        dao.addJEPLListener(resultSetListener);          
    }    
    
    public JEPLDAO<Contact> getJEPLDAO()
    {
        return dao;
    }
    
    public void insert(Contact contact)
    {
        int key = dao.createJEPLDALQuery(
                 jooqCtx.insertInto(table("CONTACT"),field("EMAIL"),field("NAME"),field("PHONE"))
                         .values("email","name", "phone").getSQL()) // INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)
                .addParameters(contact.getEmail(),contact.getName(),contact.getPhone())
                .getGeneratedKey(int.class);
        contact.setId(key);
    }     
    
    public void insertImplicitUpdateListener(Contact contact)
    {
        int key = dao.insert(contact).getGeneratedKey(int.class);
        contact.setId(key);
    }        
    
    public void insertExplicitResultSetListener(Contact contact)
    {
        // Just to show how data conversion can be possible if required
        JEPLResultSetDALListener listener = new JEPLResultSetDALListener()
        {
            @Override
            public void setupJEPLResultSet(JEPLResultSet jrs,JEPLTask<?> task) throws Exception { 
            }
            @Override
            @SuppressWarnings("unchecked")
            public <U> U getValue(int columnIndex, Class<U> returnType, JEPLResultSet jrs) throws Exception
            {
                if (!returnType.equals(int.class)) throw new RuntimeException("UNEXPECTED");
                // Expected columnIndex = 1 (only one row and one column is expected)
                ResultSet rs = jrs.getResultSet();
                int resInt = rs.getInt(columnIndex);
                Object resObj = rs.getObject(columnIndex);
                Integer resIntObj = (Integer)jrs.getJEPLStatement().getJEPLDAL().cast(resObj, returnType);
                if (resInt != resIntObj) throw new RuntimeException("UNEXPECTED");
                return (U)resIntObj; 
            }
        };

        int key = dao.createJEPLDALQuery(
                    jooqCtx.insertInto(table("CONTACT"),field("EMAIL"),field("NAME"),field("PHONE"))
                            .values("email","name", "phone").getSQL()) // INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?, ?, ?)                
                    .addParameters(contact.getEmail(),contact.getName(),contact.getPhone())
                    .addJEPLListener(listener)
                    .getGeneratedKey(int.class);
         contact.setId(key);
    }    
    
    public void insertUsingNamedParams(Contact contact)
    {
        int key = dao.createJEPLDALQuery( "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (:email,:name,:phone)" )                 
                        .setParameter("email",contact.getEmail())
                        .setParameter("name",contact.getName())
                        .setParameter("phone",contact.getPhone())
                        .getGeneratedKey(int.class);
         contact.setId(key);
    }     
    
    public void insertUsingNumberedParams(Contact contact)
    {
        int key = dao.createJEPLDALQuery( "INSERT INTO CONTACT (EMAIL, NAME, PHONE) VALUES (?1,?2,?3)" )                 
                        .setParameter(1,contact.getEmail())
                        .setParameter(2,contact.getName())
                        .setParameter(3,contact.getPhone())
                        .getGeneratedKey(int.class);
         contact.setId(key);             
    }    
    
    public boolean update(Contact contact)
    {
        int updated = dao.createJEPLDALQuery(
                jooqCtx.update(table("CONTACT"))
                        .set(field("EMAIL"), "email")
                        .set(field("NAME"),  "name")
                        .set(field("PHONE"), "phone")
                        .where(field("ID").equal(0)).getSQL()) // "UPDATE CONTACT SET EMAIL = ?, NAME = ?, PHONE = ? WHERE ID = ?")
                .addParameters(contact.getEmail(),contact.getName(),contact.getPhone(),contact.getId())
                .executeUpdate();
        return updated > 0;
    }    
    
    public boolean updateImplicitUpdateListener(Contact contact)
    {
        int updated = dao.update(contact)
                .executeUpdate();
        return updated > 0;
    }        
    
    public boolean delete(Contact contact)
    {
        return deleteById(contact.getId());
    }
    
    public boolean deleteById(int id)
    {
        int deleted = dao.createJEPLDALQuery( jooqCtx.delete(table("CONTACT")).where(field("ID").equal(0)).getSQL() ) // "DELETE FROM CONTACT WHERE ID = ?" 
                        .addParameters(id)             
                        .executeUpdate();     
        return deleted > 0;
    }    
    
    public boolean deleteImplicitUpdateListener(Contact contact)
    {
        int deleted = dao.delete( contact )       
                        .executeUpdate();     
        return deleted > 0;
    }    
    
    public int deleteAll()
    {
        return dao.createJEPLDALQuery( jooqCtx.delete(table("CONTACT")).getSQL() ) // "DELETE FROM CONTACT" 
                            .executeUpdate();      
    }    
  
    
    public JEPLResultSetDAO<Contact> selectActiveResult()
    {
        return dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).getSQL() ) // "SELECT * FROM CONTACT"
                    .getJEPLResultSetDAO();    
    }                 
    
    public static Contact[] toContactArray(JEPLResultSetDAO<Contact> list)
    {
        if (list.isClosed()) throw new RuntimeException("Unexpected");
        int size = list.size();
        Contact[] res = ((List<Contact>)list).toArray(new Contact[size]); // JEPLResultSetDAO implements List interface
        if (!list.isClosed()) throw new RuntimeException("Unexpected");
        return res;
    }         
    
    public List<Contact> selectNotActiveResult()
    {
        // "ORDER BY ID" is not really needed, is just to play with jooq
        List<Contact> list = dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                .getResultList();       
        return list;
    }            
    
    public List<Contact> selectNotActiveResult(int maxResults)
    {
        // "ORDER BY" is not really needed, is just to play with jooq
        List<Contact> list = dao.createJEPLDAOQuery( jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID"),field("NAME")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID,NAME"
                .setMaxResults(maxResults)
                .getResultList();       
        return list;
    }             
    
    public List<Contact> selectNotActiveResult2(final int maxResults)
    {
        // Another (verbose) approach using JDBC        
        JEPLPreparedStatementListener<List<Contact>> listener = (JEPLPreparedStatement jstmt,JEPLTask<List<Contact>> task) -> { // void setupJEPLPreparedStatement(...) throws Exception

            PreparedStatement stmt = jstmt.getPreparedStatement();
            int old = stmt.getMaxRows();
            stmt.setMaxRows(maxResults);
            try
            {
                List<Contact> res = task.exec();
            }
            finally
            {
                stmt.setMaxRows(old); // Restore
            }            
        };

        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).getSQL())
                .addJEPLListener(listener)
                .getResultList();
    }    
    
    public int selectCount()  // selectCount method name is used instead "count" to avoid name clashing with jooq "org.jooq.impl.DSL.count()"
    {
        return dao.createJEPLDALQuery( jooqCtx.select(count()).from(table("CONTACT")).getSQL() )
                            .getOneRowFromSingleField(int.class);     
    }
    
    public List<Contact> selectRange(int from,int to)
    {
        // The simplest form
        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                .setFirstResult(from)
                .setMaxResults(to - from)
                .getResultList();
    }    
    
    public List<Contact> selectRange2(final int from,final int to)
    {
        // Another (verbose) approach using JDBC
        JEPLResultSetDAOListener<Contact> listener = new JEPLResultSetDAOListener<Contact>()
        {
            @Override
            public  void setupJEPLResultSet(JEPLResultSet jrs,JEPLTask<?> task) throws Exception
            {
                resultSetListener.setupJEPLResultSet(jrs, task);
                
                ResultSet rs = jrs.getResultSet();
                rs.absolute(from); // Is not still closed

                // Not needed, just to know the state of ResultSet and some demostrative check:
                @SuppressWarnings("unchecked")
                List<Contact> res = (List<Contact>)task.exec(); 
                if (res.size() > to - from) throw new RuntimeException("Unexpected");
            }

            @Override
            public Contact createObject(JEPLResultSet jrs) throws Exception
            {
                return resultSetListener.createObject(jrs);
            }

            @Override
            public void fillObject(Contact obj,JEPLResultSet jrs) throws Exception
            {
                resultSetListener.fillObject(obj, jrs);
                
                ResultSet rs = jrs.getResultSet();
                int row = rs.getRow(); 
                if (row + 1 == to)
                    jrs.stop();
            }
        };

        return dao.createJEPLDAOQuery(jooqCtx.selectFrom(table("CONTACT")).orderBy(field("ID")).getSQL() ) // "SELECT * FROM CONTACT ORDER BY ID"
                .addJEPLListener(listener)
                .getResultList();
    }    
}
