import java.io.*;
import java.sql.*;

public class MyStatements
{
	static Connection conn;
	
	public MyStatements()
	{
		try {
			conn = JDBCConnect.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addCandidate(String firstName,String lastName,Date dob, 
            String email, String phone, int[] skills) {
		// for insert a new candidate
		PreparedStatement pstmt = null;

		// for assign skills to candidate
		PreparedStatement pstmtAssignment = null;

		// for getting candidate id
		ResultSet rs = null;

		try {
			// set auto commit to false
			conn.setAutoCommit(false);
			// 
			// Insert candidate
			// 
			String sqlInsert = "INSERT INTO candidates(first_name,last_name,dob,phone,email) "
					+ "VALUES(?,?,?,?,?)";

			pstmt = conn.prepareStatement(sqlInsert,Statement.RETURN_GENERATED_KEYS);

			pstmt.setString(1, firstName);
			pstmt.setString(2, lastName);
			pstmt.setDate(3, dob);
			pstmt.setString(4, phone);
			pstmt.setString(5, email);

			int rowAffected = pstmt.executeUpdate();

			// get candidate id
			rs = pstmt.getGeneratedKeys();
			int candidateId = 0;
			if(rs.next())
				candidateId = rs.getInt(1);
			
			// in case the insert operation successes, assign skills to candidate
			if(rowAffected == 1)
			{
				// assign skills to candidates
				String sqlPivot = "INSERT INTO candidate_skills(candidate_id,skill_id) "
						+ "VALUES(?,?)";
				
				pstmtAssignment = conn.prepareStatement(sqlPivot);
				for(int skillId : skills) {
					
					pstmtAssignment.setInt(1, candidateId);
					pstmtAssignment.setInt(2, skillId);

					pstmtAssignment.executeUpdate();
				}
				conn.commit();
			} else {
				conn.rollback();
			}
		} catch (SQLException ex) {
			// roll back the transaction
			try{
				if(conn != null)
					conn.rollback();
			}catch(SQLException e){
				System.out.println(e.getMessage());
			}

			System.out.println(ex.getMessage());
		} finally {
			try {
				if(rs != null)  rs.close();
				if(pstmt != null) pstmt.close();
				if(pstmtAssignment != null) pstmtAssignment.close();
				if(conn != null) conn.close();

			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	public void query(String id){
		String sql;
		if("all".equals(id))
			sql = String.format("SELECT * " +
	                "FROM candidates");
		else
				sql = String.format("SELECT * " +
                "FROM candidates where id=%s",id);

    	try (
    			Statement stmt  = conn.createStatement();
    			ResultSet rs    = stmt.executeQuery(sql);) {
      
    		// loop through the result set
    		while (rs.next())
    		{
    			System.out.println(rs.getString("first_name") + "\t" + 
                        rs.getString("last_name")  + "\t" +
                        rs.getString("email") + "\t" +
                        rs.getString("dob") + "\t" +
                        //rs.getString("resume") + "\t" +
                        rs.getString("phone"));
    		}
    	} 
    	catch (SQLException ex)
    	{
    		System.out.println(ex.getMessage());
    	}
    }
    
    public void update() {
    	 
        String sqlUpdate = "UPDATE candidates "
                + "SET last_name = ? "
                + "WHERE id = ?";
 
        try(PreparedStatement pstmt = conn.prepareStatement(sqlUpdate);)
        {
 
            // prepare data for update
            String lastName = "William";
            int id = 100;
            pstmt.setString(1, lastName);
            pstmt.setInt(2, id);
 
            int rowAffected = pstmt.executeUpdate();
            System.out.println(String.format("Row affected %d", rowAffected));
 
            // reuse the prepared statement
            lastName = "Grohe";
            id = 101;
            pstmt.setString(1, lastName);
            pstmt.setInt(2, id);
 
            rowAffected = pstmt.executeUpdate();
            System.out.println(String.format("Row affected %d", rowAffected));
 
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
    
    public int insertCandidate(String firstName,String lastName,Date dob, 
            String email, String phone) {
    	
    	// for insert a new candidate
    	ResultSet rs = null;
    	int candidateId = 0;

    	String sql = "INSERT INTO candidates(first_name,last_name,dob,phone,email) "
    			+ "VALUES(?,?,?,?,?)";

		try (PreparedStatement pstmt = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS);)
		{

    		// set parameters for statement
    		pstmt.setString(1, firstName);
    		pstmt.setString(2, lastName);
    		pstmt.setDate(3, dob);
    		pstmt.setString(4, phone);
    		pstmt.setString(5, email);

    		int rowAffected = pstmt.executeUpdate();
    		if(rowAffected == 1)
    		{
    			// get candidate id
    			rs = pstmt.getGeneratedKeys();
    			if(rs.next())
    				candidateId = rs.getInt(1);
    			System.out.println("inserted succesfully!");
    		}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		finally {
    		try {
    			if(rs != null)  rs.close();
    		} catch (SQLException e) {
    			System.out.println(e.getMessage());
    		}
		}
    	return candidateId;
    }

    public void getSkills(int candidateId) {
        // 
        String query = "{ call get_candidate_skill(?) }";
        ResultSet rs;
 
        try (CallableStatement stmt = conn.prepareCall(query)) {
 
            stmt.setInt(1, candidateId);
 
            rs = stmt.executeQuery();
            while (rs.next()) {
                System.out.println(String.format("%s - %s",
                        rs.getString("first_name") + " "
                        + rs.getString("last_name"),
                        rs.getString("skill")));
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public void writeBlob(int candidateId, String filename) {
        // update sql
        String updateSQL = "UPDATE candidates "
                + "SET resume = ? "
                + "WHERE id=?";
 
        try (PreparedStatement pstmt = conn.prepareStatement(updateSQL)) {
 
            // read the file
            File file = new File(filename);
            FileInputStream input = new FileInputStream(file);
 
            // set parameters
            pstmt.setBinaryStream(1, input);
            pstmt.setInt(2, candidateId);
 
            // store the resume file in database
            System.out.println("Reading file " + file.getAbsolutePath());
            System.out.println("Store file in the database.");
            pstmt.executeUpdate();
 
        } catch (SQLException | FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    public void readBlob(int candidateId, String filename) {
        // update sql
        String selectSQL = "SELECT resume FROM candidates WHERE id=?";
        ResultSet rs = null;
 
        try (
                PreparedStatement pstmt = conn.prepareStatement(selectSQL);) {
            // set parameter;
            pstmt.setInt(1, candidateId);
            rs = pstmt.executeQuery();
 
            // write binary stream into file
            File file = new File(filename);
            FileOutputStream output = new FileOutputStream(file);
 
            System.out.println("Writing to file " + file.getAbsolutePath());
            while (rs.next()) {
                InputStream input = rs.getBinaryStream("resume");
                byte[] buffer = new byte[1024];
                while (input.read(buffer) > 0) {
                    output.write(buffer);
                }
            }
        } catch (SQLException | IOException e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        } 
    }

}