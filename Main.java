import java.sql.Date;

public class Main {

    public static void main(String[] args)
    {
    	MyStatements mystatement = new MyStatements();
    	
    	/*
    	mystatement.query("all");
    	
    	mystatement.query("135");
		mystatement.update();
		
		int id = mystatement.insertCandidate("Bush", "Lily", Date.valueOf("1980-01-04"), 
		        "bush.l@yahoo.com", "(408) 898-6666");
		mystatement.query(id);
		
    	int[] skills = {1,2,3};
        mystatement.addCandidate("John", "Doe", Date.valueOf("1990-01-04"), 
                        "john.d@yahoo.com", "(408) 898-5641", skills);
   
    	mystatement.getSkills(135);
    	
    	mystatement.writeBlob(121, "johndoe_resume.pdf");
    	
    	mystatement.query("all");
    	*/
    	mystatement.readBlob(121, "johndoe_resume_from_db.pdf");
    } 
}
