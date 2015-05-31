
package example.jeplayer.groovydsl




    def takeV1(n) 
    { 
        [ of:{ drug -> 
                [after : { time -> println "TIME " + time }]
             }   
        ]
    }

    def execV1() 
    {
        Integer.metaClass.getPills { -> delegate }
        Integer.metaClass.getHours { -> delegate }

        def chloroquinine = 1   


        takeV1 2.pills of chloroquinine after 6.hours 

        takeV1(2.pills).of(chloroquinine).after(6.hours)
        takeV1(2.pills)['of'](chloroquinine)['after'](6.hours) // (chloroquinine)] after(6.hours)        
    }

    def takeV2(n) 
    { 
        [ pills : { of -> 
                    [ chloroquinine : 
                        { after ->  
                            ['6': { time -> }]
                        } 
                    ]
                  }
        ]
    }


    def execV2() 
    {
        // def (of, after, hours)  = 1
        def of = 1, after = 1, hours = 1
    
        takeV2 2 pills of chloroquinine after 6 hours 

        takeV2(2).pills(of).chloroquinine(after)['6'](hours)
        takeV2(2)['pills'](of)['chloroquinine'](after)['6'](hours) //(of)['chloroquinine'](after)['6'](hours) // (chloroquinine)] after(6.hours)               

    }



