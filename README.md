# vaultaire-query

Query over a producer is just a 'ListT'. To construct a query:

  * Use 'Select' on a 'Producer', see the 'Pipes' documentation.
  * Use list comprehensions syntax, example:

      ```
      query :: Query m (Person, Role)
      query = [ (person, role)
              | person <- Select $ people
              , age person < 30
              , role   <- possibleRoles ]

      people        :: Producer Person m ()
      possibleRoles :: Query m Role
      ```

    The above query construct a product of ``Person`` and ``Role``, SQL-style.
