use incautacionesPruebas;


const session = db.getMongo().startSession();

const evento = session.getDatabase("incautacionesPruebas");

session.startTransaction();

// Encuentra todos los municipios que empiezan por “San”.
evento.municipios.find({ nomMuni: { $regex: /^San/i } });

//Lista los municipios que terminan en “ito”.
evento.municipios.aggregate([
    {
      $match:{
        "nomMuni": { $regex: /ito$/i }
      }
    },
    {
      $sort: { nomMuni: 1}
    },
    {
        $project: {
            _id: 0,
            nomMuni:1
        }
    }
  ]);

// Busca los municipios cuyo nombre contenga la palabra “Valle”.
evento.municipios.find({ nomMuni: { $regex: /valle/i } });


session.commitTransaction();

session.endSession();



pruebita.municipios.aggregate([
    {
        $match:{

        }
    }
]);
