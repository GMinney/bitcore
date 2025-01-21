db = db.getSiblingDB("thoughtcore");

db.createUser({
    user: "username",
    pwd: "password",
    roles: [
      {
        role: 'readWrite', 
        db: 'thoughtcore'
      },
    ],
  });