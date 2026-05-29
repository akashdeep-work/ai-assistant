router_examples = [
    # ==========================================
    # RAG AGENT (Internal Data, Policies, DB Records)
    # ==========================================
    
    # HR & Policies
    ("What is the company policy on working from home?", "rag_agent"),
    ("WFH policy guidelines", "rag_agent"),
    ("How many PTO days do I have left?", "rag_agent"),
    ("Check my sick leave balance", "rag_agent"),
    ("Maternity and paternity leave rules", "rag_agent"),
    ("How do I expense a client dinner?", "rag_agent"),
    ("Show me the employee handbook", "rag_agent"),
    ("What are the core company values?", "rag_agent"),
    ("Company holiday schedule for this year", "rag_agent"),
    
    # Internal IT & Procedures
    ("How do I connect to the corporate VPN?", "rag_agent"),
    ("Reset my internal portal password", "rag_agent"),
    ("Where can I find the brand guidelines?", "rag_agent"),
    ("Onboarding documentation for new hires", "rag_agent"),
    ("How to request a new laptop from IT", "rag_agent"),
    ("Search the internal database for project specs", "rag_agent"),
    
    # Personal & Employee Records
    ("Look up my employee records", "rag_agent"),
    ("Show my latest performance review", "rag_agent"),
    ("Who is listed as my direct manager?", "rag_agent"),
    ("Update my emergency contact information", "rag_agent"),
    ("Where is my latest pay stub?", "rag_agent"),
    
    # System/Database Specific (Keywords)
    ("Database query for user 1045", "rag_agent"),
    ("Check private documents for 'Project Nova'", "rag_agent"),
    ("Retrieve the Q3 financial report", "rag_agent"),
    ("Internal wiki search", "rag_agent"),

    # ==========================================
    # CHAT AGENT (General Knowledge, Small Talk, Tasks)
    # ==========================================
    
    # Greetings & Small Talk
    ("Hello there!", "chat_agent"),
    ("Good morning", "chat_agent"),
    ("How are you doing today?", "chat_agent"),
    ("Who created you?", "chat_agent"),
    ("Are you a robot or a human?", "chat_agent"),
    ("Thanks for your help!", "chat_agent"),
    ("Goodbye, talk to you later", "chat_agent"),
    ("What's up?", "chat_agent"),
    
    # General Knowledge & Trivia
    ("What is the capital of France?", "chat_agent"),
    ("Who won the world series in 2022?", "chat_agent"),
    ("How far away is the moon?", "chat_agent"),
    ("Explain the theory of relativity to a 5 year old", "chat_agent"),
    ("What is the speed of light?", "chat_agent"),
    ("Give me a brief history of Rome", "chat_agent"),
    ("How do airplanes fly?", "chat_agent"),
    
    # Generative Tasks & Formatting
    ("Write a python script to reverse a string", "chat_agent"),
    ("Tell me a joke about programmers", "chat_agent"),
    ("Write a poem about space exploration", "chat_agent"),
    ("Translate 'hello' into Spanish", "chat_agent"),
    ("Summarize the plot of Hamlet", "chat_agent"),
    ("Give me a recipe for chocolate chip cookies", "chat_agent"),
    ("Brainstorm 5 names for a tech startup", "chat_agent"),
    
    # Math & Logic (No tools required)
    ("What is 250 divided by 4?", "chat_agent"),
    ("If I have 3 apples and eat 1, how many are left?", "chat_agent"),
    ("Calculate 15% tip on a $45 bill", "chat_agent")
]